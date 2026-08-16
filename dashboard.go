package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/saveweb/go2internetarchive/pkg/upload"
)

type dashboardProject struct {
	Name                  string
	PendingArtifacts      int64
	PendingBytes          int64
	TriggerBytes          int64
	BytesUntilTrigger     int64
	TriggerPercent        int
	OldestAcceptedAt      int64
	SecondsUntilMaxWait   int64
	BuildingPackages      int64
	RetryingPackages      int64
	PackageBuildErrors    []dashboardPackageError
	NextPackageRetryIn    int64
	BlockedPackages       int64
	PackagingErrors       int64
	DeliveriesPending     int64
	DeliveriesActive      int64
	DeliveriesRetrying    int64
	DeliveriesDelivered   int64
	DeliveryProgress      bool
	DeliveryBytes         int64
	DeliveryTotalBytes    int64
	DeliveryBytesPerSec   int64
	DeliveryFiles         int
	DeliveryTotalFiles    int
	DeliveryCurrentFile   string
	DeliveryPercent       int
	UploadingArtifacts    int64
	UploadingBytes        int64
	UploadingTotalBytes   int64
	StaleUploads          int64
	StaleUploadBytes      int64
	StaleUploadTotalBytes int64
}

type dashboardPackageError struct {
	PackageID string
	Message   string
	RetryIn   int64
}

type dashboardView struct {
	UpdatedAt        time.Time
	Projects         []dashboardProject
	UploadingCount   int64
	StaleUploadCount int64
	StaleStatsReady  bool
	StaleStatsAt     time.Time
	BuildingCount    int64
	DeliveringCount  int64
}

func (s *server) dashboard(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardPageTemplate.Execute(response, nil); err != nil {
		slogDashboardError(response, err)
	}
}

func (s *server) dashboardStatus(response http.ResponseWriter, request *http.Request) {
	if s.deliveryStore == nil {
		http.Error(response, "Delivery index unavailable", http.StatusServiceUnavailable)
		return
	}
	view, err := s.dashboardView(request.Context())
	if err != nil {
		http.Error(response, "Could not read current status", http.StatusServiceUnavailable)
		return
	}
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardStatusTemplate.Execute(response, view); err != nil {
		slogDashboardError(response, err)
	}
}

func (s *server) dashboardView(ctx context.Context) (dashboardView, error) {
	now := s.now()
	uploads, err := s.activeUploads()
	if err != nil {
		return dashboardView{}, err
	}
	byProject := make(map[string][]partialUpload)
	for _, upload := range uploads {
		byProject[upload.Project] = append(byProject[upload.Project], upload)
	}
	projectNames := make([]string, 0, len(s.cfg.Projects))
	for project := range s.cfg.Projects {
		projectNames = append(projectNames, project)
	}
	sort.Strings(projectNames)
	view := dashboardView{UpdatedAt: now}
	stale := s.staleUploads.Load()
	if stale != nil {
		view.StaleStatsReady = true
		view.StaleStatsAt = stale.ScannedAt
	}
	for _, project := range projectNames {
		projectView, err := s.deliveryStore.dashboardProject(ctx, project, s.cfg.Projects[project], now)
		if err != nil {
			return dashboardView{}, err
		}
		for _, upload := range byProject[project] {
			projectView.UploadingArtifacts++
			projectView.UploadingBytes += upload.Received
			projectView.UploadingTotalBytes += upload.Size
		}
		if stale != nil {
			stats := stale.Projects[project]
			projectView.StaleUploads = stats.Count
			projectView.StaleUploadBytes = stats.Received
			projectView.StaleUploadTotalBytes = stats.TotalBytes
		}
		view.UploadingCount += projectView.UploadingArtifacts
		view.StaleUploadCount += projectView.StaleUploads
		view.BuildingCount += projectView.BuildingPackages
		view.DeliveringCount += projectView.DeliveriesActive
		view.Projects = append(view.Projects, projectView)
	}
	return view, nil
}

func (s *deliveryStore) dashboardProject(ctx context.Context, project string, cfg projectConfig, now time.Time) (dashboardProject, error) {
	view := dashboardProject{Name: project, TriggerBytes: cfg.Packaging.TriggerBytes}
	var oldest *int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(size_bytes),0),MIN(accepted_at) FROM artifacts WHERE project=? AND package_id IS NULL AND packaging_error IS NULL`, project).Scan(&view.PendingArtifacts, &view.PendingBytes, &oldest); err != nil {
		return dashboardProject{}, err
	}
	if oldest != nil {
		view.OldestAcceptedAt = *oldest
	}
	if cfg.Packaging.Type == "identity" {
		view.TriggerBytes = 0
		view.BytesUntilTrigger = 0
		view.TriggerPercent = 100
		view.SecondsUntilMaxWait = 0
	} else {
		view.BytesUntilTrigger = max(cfg.Packaging.TriggerBytes-view.PendingBytes, 0)
		if cfg.Packaging.TriggerBytes > 0 {
			view.TriggerPercent = int(min(view.PendingBytes*100/cfg.Packaging.TriggerBytes, 100))
		}
		if oldest != nil {
			deadline := time.Unix(*oldest, 0).Add(cfg.Packaging.maxWait)
			remaining := deadline.Sub(now)
			view.SecondsUntilMaxWait = max(int64((remaining+time.Second-1)/time.Second), 0)
		}
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifacts WHERE project=? AND packaging_error IS NOT NULL`, project).Scan(&view.PackagingErrors); err != nil {
		return dashboardProject{}, err
	}
	var nextPackageRetryAt *int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FILTER (WHERE build_error IS NULL),COUNT(*) FILTER (WHERE build_error IS NOT NULL),MIN(CASE WHEN build_error IS NOT NULL THEN next_build_at END) FROM packages WHERE project=? AND state='building'`, project).Scan(&view.BuildingPackages, &view.RetryingPackages, &nextPackageRetryAt); err != nil {
		return dashboardProject{}, err
	}
	if nextPackageRetryAt != nil {
		view.NextPackageRetryIn = max(*nextPackageRetryAt-now.Unix(), 0)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT package_id,build_error,next_build_at FROM packages WHERE project=? AND state='building' AND build_error IS NOT NULL ORDER BY updated_at DESC,package_id`, project)
	if err != nil {
		return dashboardProject{}, err
	}
	for rows.Next() {
		var issue dashboardPackageError
		var retryAt int64
		if err := rows.Scan(&issue.PackageID, &issue.Message, &retryAt); err != nil {
			rows.Close()
			return dashboardProject{}, err
		}
		issue.PackageID = issue.PackageID[:min(len(issue.PackageID), 12)]
		issue.RetryIn = max(retryAt-now.Unix(), 0)
		view.PackageBuildErrors = append(view.PackageBuildErrors, issue)
	}
	if err := rows.Close(); err != nil {
		return dashboardProject{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM packages WHERE project=? AND state='blocked'`, project).Scan(&view.BlockedPackages); err != nil {
		return dashboardProject{}, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT d.state,COUNT(*) FROM deliveries d JOIN packages p ON p.package_id=d.package_id WHERE p.project=? GROUP BY d.state`, project)
	if err != nil {
		return dashboardProject{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return dashboardProject{}, err
		}
		switch state {
		case "pending":
			view.DeliveriesPending = count
		case "delivering":
			view.DeliveriesActive = count
		case "retry_wait":
			view.DeliveriesRetrying = count
		case "delivered":
			view.DeliveriesDelivered = count
		}
	}
	if err := rows.Close(); err != nil {
		return dashboardProject{}, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT d.progress FROM deliveries d JOIN packages p ON p.package_id=d.package_id WHERE p.project=? AND d.state='delivering' AND d.progress IS NOT NULL`, project)
	if err != nil {
		return dashboardProject{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return dashboardProject{}, err
		}
		var progress upload.Progress
		if err := json.Unmarshal([]byte(raw), &progress); err != nil {
			return dashboardProject{}, fmt.Errorf("decode delivery progress: %w", err)
		}
		view.DeliveryProgress = true
		view.DeliveryBytes += progress.BytesUploaded
		view.DeliveryTotalBytes += progress.TotalBytes
		view.DeliveryBytesPerSec += progress.BytesPerSecond
		view.DeliveryFiles += progress.FilesUploaded
		view.DeliveryTotalFiles += progress.TotalFiles
		if view.DeliveriesActive == 1 {
			view.DeliveryCurrentFile = progress.CurrentFile
		}
	}
	if view.DeliveryTotalBytes > 0 {
		view.DeliveryPercent = int(min(view.DeliveryBytes*100/view.DeliveryTotalBytes, 100))
	}
	return view, rows.Err()
}

func (s *server) activeUploads() ([]partialUpload, error) {
	var uploads []partialUpload
	for _, objectID := range s.activeUploadIDs() {
		upload, ok, err := s.partialUpload(objectID)
		if err != nil {
			return nil, err
		}
		if ok {
			uploads = append(uploads, upload)
		}
	}
	return uploads, nil
}

func slogDashboardError(response http.ResponseWriter, err error) {
	slog.Error("render dashboard", "err", err)
	http.Error(response, "Could not render current status", http.StatusInternalServerError)
}

var dashboardPageTemplate = template.Must(template.New("dashboard").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Canner status</title>
<script src="https://cdn.jsdelivr.net/npm/htmx.org@2.0.10/dist/htmx.min.js" integrity="sha384-H5SrcfygHmAuTDZphMHqBJLc3FhssKjG7w/CeCpFReSfwBWDTKpkzPP8c+cLsK+V" crossorigin="anonymous"></script>
<style>
:root{color-scheme:light dark;--bg:#f5f6f7;--surface:#fff;--text:#1c2024;--muted:#687078;--line:#d9dde1;--accent:#1769aa;--ok:#237a45;--warn:#9a6700;--danger:#b42318}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,sans-serif;letter-spacing:0}header{height:56px;background:#202428;color:#fff;display:flex;align-items:center;padding:0 28px;justify-content:space-between}header strong{font-size:17px}header span{color:#b8bec4;font-size:12px}main{max-width:1280px;margin:0 auto;padding:28px}.summary{display:flex;gap:32px;padding:0 0 22px;border-bottom:1px solid var(--line)}.metric b{display:block;font-size:24px}.metric span,.muted{color:var(--muted)}h1{font-size:20px;margin:26px 0 12px}.table-wrap{overflow-x:auto;border:1px solid var(--line);background:var(--surface)}table{border-collapse:collapse;width:100%;min-width:1000px}th,td{text-align:left;padding:12px 14px;border-bottom:1px solid var(--line);vertical-align:top}th{font-size:12px;color:var(--muted);background:#eef0f2;font-weight:600}tbody tr:last-child td{border-bottom:0}.project{font-weight:700}.number{font-variant-numeric:tabular-nums}.state{display:inline-flex;align-items:center;gap:6px}.dot{width:8px;height:8px;border-radius:50%;background:var(--muted)}.active .dot{background:var(--accent)}.ok .dot{background:var(--ok)}.warn .dot{background:var(--warn)}.danger .dot{background:var(--danger)}progress{display:block;width:180px;height:7px;margin:7px 0 4px;accent-color:var(--accent)}.filename{display:block;max-width:220px;overflow-wrap:anywhere}.package-issues{margin-top:6px;max-width:320px}.package-issues summary{cursor:pointer}.package-issue{margin-top:8px;overflow-wrap:anywhere}.package-issue code{display:block;color:var(--muted)}.foot{display:flex;justify-content:space-between;margin-top:10px;color:var(--muted);font-size:12px}.error{padding:20px;border:1px solid var(--danger);color:var(--danger);background:var(--surface)}@media(max-width:700px){header{padding:0 16px}main{padding:18px}.summary{gap:20px;overflow-x:auto}.metric b{font-size:20px}}
@media(prefers-color-scheme:dark){:root{--bg:#151719;--surface:#1d2023;--text:#edf0f2;--muted:#a1a8ae;--line:#363b40}th{background:#25292d}}
</style></head><body><header><strong>Canner</strong><span>Artifact packaging and delivery</span></header><main><div id="status" hx-get="/dashboard/status" hx-trigger="load, every 2s" hx-swap="innerHTML"><div class="muted">Loading current status...</div></div></main></body></html>`))

var dashboardStatusTemplate = template.Must(template.New("status").Funcs(template.FuncMap{
	"bytes": formatBytes, "duration": formatDuration,
}).Parse(`<div class="summary"><div class="metric"><b class="number">{{.UploadingCount}}</b><span>active uploads</span></div><div class="metric">{{if .StaleStatsReady}}<b class="number">{{.StaleUploadCount}}</b>{{else}}<b>Scanning...</b>{{end}}<span>stale/incomplete</span></div><div class="metric"><b class="number">{{.BuildingCount}}</b><span>packages building</span></div><div class="metric"><b class="number">{{.DeliveringCount}}</b><span>deliveries active</span></div></div><h1>Projects</h1><div class="table-wrap"><table><thead><tr><th>Project</th><th>Artifact backlog</th><th>Next trigger</th><th>Uploading</th><th>Packaging</th><th>Delivery</th><th>Issues</th></tr></thead><tbody>{{range .Projects}}<tr><td class="project">{{.Name}}</td><td><span class="number">{{.PendingArtifacts}}</span> artifacts<br><span class="muted number">{{bytes .PendingBytes}}</span></td><td>{{if eq .TriggerBytes 0}}Immediate{{else}}<span class="number">{{bytes .BytesUntilTrigger}}</span> remaining<progress value="{{.TriggerPercent}}" max="100"></progress><span class="muted">{{if .PendingArtifacts}}or {{duration .SecondsUntilMaxWait}}{{else}}waiting for artifacts{{end}}</span>{{end}}</td><td>{{if .UploadingArtifacts}}<span class="state active"><span class="dot"></span><span class="number">{{.UploadingArtifacts}} active</span></span><br><span class="muted number">{{bytes .UploadingBytes}} / {{bytes .UploadingTotalBytes}}</span>{{else}}<span class="state"><span class="dot"></span>Idle</span>{{end}}{{if .StaleUploads}}<br><span class="state warn"><span class="dot"></span><span class="number">{{.StaleUploads}} stale/incomplete</span></span><br><span class="muted number">{{bytes .StaleUploadBytes}} / {{bytes .StaleUploadTotalBytes}}</span>{{end}}</td><td>{{if .BuildingPackages}}<span class="state active"><span class="dot"></span><span class="number">{{.BuildingPackages}} building</span></span>{{end}}{{if .RetryingPackages}}{{if .BuildingPackages}}<br>{{end}}<span class="state warn"><span class="dot"></span><span class="number">{{.RetryingPackages}} waiting to retry</span></span><br><span class="muted">{{if .NextPackageRetryIn}}retry in {{duration .NextPackageRetryIn}}{{else}}retry due{{end}}</span>{{end}}{{if .BlockedPackages}}{{if or .BuildingPackages .RetryingPackages}}<br>{{end}}<span class="state danger"><span class="dot"></span><span class="number">{{.BlockedPackages}} blocked</span></span>{{end}}{{if not (or .BuildingPackages .RetryingPackages .BlockedPackages)}}<span class="state"><span class="dot"></span>Idle</span>{{end}}</td><td>{{if .DeliveriesActive}}<span class="state active"><span class="dot"></span><span class="number">{{.DeliveriesActive}} uploading</span></span>{{if .DeliveryProgress}}<progress value="{{.DeliveryPercent}}" max="100"></progress><span class="muted number">{{bytes .DeliveryBytes}} / {{bytes .DeliveryTotalBytes}} · {{bytes .DeliveryBytesPerSec}}/s</span><br><span class="muted number">{{.DeliveryFiles}} / {{.DeliveryTotalFiles}} files</span>{{if .DeliveryCurrentFile}}<span class="muted filename">{{.DeliveryCurrentFile}}</span>{{end}}{{else}}<br><span class="muted">Starting...</span>{{end}}{{else}}<span class="state ok"><span class="dot"></span>Idle</span>{{end}}<br><span class="muted number">{{.DeliveriesPending}} pending · {{.DeliveriesRetrying}} retrying · {{.DeliveriesDelivered}} delivered</span></td><td>{{if or .BlockedPackages .PackagingErrors .PackageBuildErrors}}<span class="state danger"><span class="dot"></span><span class="number">{{.BlockedPackages}} blocked · {{.PackagingErrors}} artifact errors · {{len .PackageBuildErrors}} package errors</span></span>{{if .PackageBuildErrors}}<details class="package-issues"><summary>Show package errors</summary>{{range .PackageBuildErrors}}<div class="package-issue"><code>package {{.PackageID}} · {{if .RetryIn}}retry in {{duration .RetryIn}}{{else}}retry due{{end}}</code>{{.Message}}</div>{{end}}</details>{{end}}{{else}}<span class="state ok"><span class="dot"></span>None</span>{{end}}</td></tr>{{end}}</tbody></table></div><div class="foot"><span>Auto-refresh every 2 seconds{{if .StaleStatsReady}} · stale scan {{.StaleStatsAt.UTC.Format "15:04:05 UTC"}}{{end}}</span><time>{{.UpdatedAt.UTC.Format "2006-01-02 15:04:05 UTC"}}</time></div>`))

func formatBytes(value int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	n := float64(value)
	unit := 0
	for n >= 1024 && unit < len(units)-1 {
		n /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", value, units[unit])
	}
	return fmt.Sprintf("%.1f %s", n, units[unit])
}

func formatDuration(seconds int64) string {
	if seconds <= 0 {
		return "ready now"
	}
	duration := time.Duration(seconds) * time.Second
	if duration >= time.Hour {
		return duration.Round(time.Minute).String()
	}
	return duration.Round(time.Second).String()
}
