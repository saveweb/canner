package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/saveweb/mergewarc"
	"github.com/zeebo/blake3"
)

type artifactRecord struct {
	ObjectID         string  `json:"object_id"`
	Project          string  `json:"project"`
	Filename         string  `json:"filename"`
	Checksum         string  `json:"checksum"`
	SizeBytes        int64   `json:"size_bytes"`
	AcceptedAt       int64   `json:"accepted_at"`
	PackageID        *string `json:"package_id,omitempty"`
	PackagingError   *string `json:"packaging_error,omitempty"`
	SourcePurgedAt   *int64  `json:"source_purged_at,omitempty"`
	NextSourcePurge  int64   `json:"next_source_purge_at"`
	SourcePurgeError *string `json:"source_purge_error,omitempty"`
}

type packageRecord struct {
	PackageID        string  `json:"package_id"`
	Project          string  `json:"project"`
	Packager         string  `json:"packager"`
	Filename         string  `json:"filename"`
	ManifestFilename string  `json:"manifest_filename"`
	State            string  `json:"state"`
	SizeBytes        *int64  `json:"size_bytes,omitempty"`
	Checksum         *string `json:"checksum,omitempty"`
	ManifestChecksum *string `json:"manifest_checksum,omitempty"`
	MemberCount      int     `json:"member_count"`
	BuildAttempts    int     `json:"build_attempts"`
	NextBuildAt      int64   `json:"next_build_at"`
	BuildError       *string `json:"build_error,omitempty"`
	CreatedAt        int64   `json:"created_at"`
	UpdatedAt        int64   `json:"updated_at"`
	SealedAt         *int64  `json:"sealed_at,omitempty"`
	PurgeAfter       *int64  `json:"purge_after,omitempty"`
	NextPurgeAt      *int64  `json:"next_purge_attempt_at,omitempty"`
	PurgeError       *string `json:"purge_error,omitempty"`
	PurgedAt         *int64  `json:"purged_at,omitempty"`
}

type deliveryRecord struct {
	PackageID   string  `json:"package_id"`
	SinkID      string  `json:"sink_id"`
	State       string  `json:"state"`
	Plan        string  `json:"-"`
	Attempts    int     `json:"attempts"`
	NextAttempt int64   `json:"next_attempt_at"`
	LastError   *string `json:"last_error,omitempty"`
	RemoteID    *string `json:"remote_id,omitempty"`
	UpdatedAt   int64   `json:"updated_at"`
	DeliveredAt *int64  `json:"delivered_at,omitempty"`
}

type packageMember struct {
	PackageID  string
	Ordinal    int
	ObjectID   string
	Offset     int64
	SizeBytes  int64
	Checksum   string
	Project    string
	Filename   string
	AcceptedAt int64
}

type packageDeliveryPlan struct {
	Version         int                 `json:"version"`
	Sink            string              `json:"sink"`
	CredentialsFile string              `json:"credentials_file"`
	Identifier      string              `json:"identifier"`
	Files           map[string]string   `json:"files"`
	Metadata        map[string][]string `json:"metadata"`
	RetentionNanos  int64               `json:"local_artifact_retention_nanos"`
}

func (s *deliveryStore) addArtifact(ctx context.Context, artifact artifactRecord) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO artifacts(object_id,project,filename,checksum,size_bytes,accepted_at) VALUES(?,?,?,?,?,?) ON CONFLICT(object_id) DO NOTHING`, artifact.ObjectID, artifact.Project, artifact.Filename, artifact.Checksum, artifact.SizeBytes, artifact.AcceptedAt)
	return err
}

func (s *deliveryStore) hasArtifact(ctx context.Context, objectID string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifacts WHERE object_id=?)`, objectID).Scan(&exists)
	return exists == 1, err
}

func (s *deliveryStore) artifact(ctx context.Context, objectID string) (artifactRecord, error) {
	var item artifactRecord
	err := s.db.QueryRowContext(ctx, `SELECT object_id,project,filename,checksum,size_bytes,accepted_at,package_id,packaging_error,source_purged_at,next_source_purge_at,source_purge_error FROM artifacts WHERE object_id=?`, objectID).Scan(&item.ObjectID, &item.Project, &item.Filename, &item.Checksum, &item.SizeBytes, &item.AcceptedAt, &item.PackageID, &item.PackagingError, &item.SourcePurgedAt, &item.NextSourcePurge, &item.SourcePurgeError)
	return item, err
}

func (s *deliveryStore) recoverInterruptedDeliveries(ctx context.Context, now int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE deliveries SET state='retry_wait',next_attempt_at=?,last_error='delivery process stopped before recording an outcome',updated_at=? WHERE state='delivering'`, now, now)
	return err
}

func (s *deliveryStore) nextBuildingPackage(ctx context.Context, now int64) (packageRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `UPDATE packages SET build_attempts=build_attempts+1,updated_at=? WHERE package_id=(SELECT package_id FROM packages WHERE state='building' AND next_build_at<=? ORDER BY created_at,package_id LIMIT 1) RETURNING `+packageColumns, now, now)
	pkg, err := scanPackage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return packageRecord{}, false, nil
	}
	return pkg, err == nil, err
}

func (s *deliveryStore) reservePackage(ctx context.Context, project string, cfg packagingConfig, delivery deliveryConfig, now time.Time) (packageRecord, bool, error) {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO packaging_projects(project,draining,updated_at) VALUES(?,0,?) ON CONFLICT(project) DO NOTHING`, project, now.Unix()); err != nil {
		return packageRecord{}, false, err
	}
	var draining bool
	if err := s.db.QueryRowContext(ctx, `SELECT draining FROM packaging_projects WHERE project=?`, project).Scan(&draining); err != nil {
		return packageRecord{}, false, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT object_id,project,filename,checksum,size_bytes,accepted_at FROM artifacts WHERE project=? AND package_id IS NULL AND packaging_error IS NULL ORDER BY accepted_at,object_id`, project)
	if err != nil {
		return packageRecord{}, false, err
	}
	var candidates []artifactRecord
	var total int64
	for rows.Next() {
		var item artifactRecord
		if err := rows.Scan(&item.ObjectID, &item.Project, &item.Filename, &item.Checksum, &item.SizeBytes, &item.AcceptedAt); err != nil {
			rows.Close()
			return packageRecord{}, false, err
		}
		candidates = append(candidates, item)
		total += item.SizeBytes
	}
	if err := rows.Close(); err != nil {
		return packageRecord{}, false, err
	}
	if len(candidates) == 0 {
		if draining {
			_, err := s.db.ExecContext(ctx, `UPDATE packaging_projects SET draining=0,updated_at=? WHERE project=?`, now.Unix(), project)
			return packageRecord{}, false, err
		}
		return packageRecord{}, false, nil
	}
	var selected []artifactRecord
	var selectedBytes int64
	if cfg.Type == "identity" {
		selected = candidates[:1]
		selectedBytes = selected[0].SizeBytes
		draining = false
	} else {
		ageTriggered := now.Sub(time.Unix(candidates[0].AcceptedAt, 0)) >= cfg.maxWait
		if total >= cfg.TriggerBytes {
			draining = true
		}
		if draining && total < cfg.TargetPackageBytes {
			draining = false
		}
		if !draining && !ageTriggered {
			return packageRecord{}, false, nil
		}
		for _, item := range candidates {
			if len(selected) > 0 && selectedBytes+item.SizeBytes > cfg.TargetPackageBytes {
				break
			}
			selected = append(selected, item)
			selectedBytes += item.SizeBytes
			if selectedBytes >= cfg.TargetPackageBytes {
				break
			}
		}
	}
	if len(selected) == 0 {
		return packageRecord{}, false, nil
	}
	packageID := derivePackageID(cfg.Type, project, selected)
	filename := packageFilename(project, packageID, cfg.Type, selected[0].Filename)
	manifestFilename := filename + ".manifest.jsonl"
	plan, err := makePackageDeliveryPlan(project, packageID, filename, manifestFilename, now, delivery)
	if err != nil {
		return packageRecord{}, false, err
	}
	planRaw, err := json.Marshal(plan)
	if err != nil {
		return packageRecord{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return packageRecord{}, false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO packages(package_id,project,packager,filename,manifest_filename,state,member_count,next_build_at,updated_at,created_at) VALUES(?,?,?,?,?,'building',?,0,?,?) ON CONFLICT(package_id) DO NOTHING`, packageID, project, cfg.Type, filename, manifestFilename, len(selected), now.Unix(), now.Unix())
	if err != nil {
		return packageRecord{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return packageRecord{}, false, err
	}
	if inserted == 0 {
		return packageRecord{}, false, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO deliveries(package_id,sink_id,state,plan,next_attempt_at,updated_at) VALUES(?,'internet_archive','pending',?,0,?)`, packageID, string(planRaw), now.Unix()); err != nil {
		return packageRecord{}, false, err
	}
	var offset int64
	for ordinal, item := range selected {
		if _, err := tx.ExecContext(ctx, `INSERT INTO package_members(package_id,ordinal,object_id,offset_bytes,size_bytes,checksum) VALUES(?,?,?,?,?,?)`, packageID, ordinal, item.ObjectID, offset, item.SizeBytes, item.Checksum); err != nil {
			return packageRecord{}, false, err
		}
		result, err := tx.ExecContext(ctx, `UPDATE artifacts SET package_id=? WHERE object_id=? AND package_id IS NULL`, packageID, item.ObjectID)
		if err != nil {
			return packageRecord{}, false, err
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return packageRecord{}, false, fmt.Errorf("reserve artifact %q affected %d rows", item.ObjectID, count)
		}
		offset += item.SizeBytes
	}
	keepDraining := cfg.Type == "mergewarc" && draining && total-selectedBytes >= cfg.TargetPackageBytes
	if _, err := tx.ExecContext(ctx, `UPDATE packaging_projects SET draining=?,updated_at=? WHERE project=?`, keepDraining, now.Unix(), project); err != nil {
		return packageRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return packageRecord{}, false, err
	}
	return packageRecord{PackageID: packageID, Project: project, Packager: cfg.Type, Filename: filename, ManifestFilename: manifestFilename, State: "building", MemberCount: len(selected), CreatedAt: now.Unix(), UpdatedAt: now.Unix()}, true, nil
}

func derivePackageID(packager, project string, artifacts []artifactRecord) string {
	h := sha256.New()
	io.WriteString(h, "canner-package-v1\x00")
	io.WriteString(h, packager)
	h.Write([]byte{0})
	io.WriteString(h, project)
	h.Write([]byte{0})
	for _, item := range artifacts {
		io.WriteString(h, item.ObjectID)
		h.Write([]byte{0})
		io.WriteString(h, item.Checksum)
		h.Write([]byte{0})
		io.WriteString(h, strconv.FormatInt(item.SizeBytes, 10))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func packageFilename(project, packageID, packager, artifactFilename string) string {
	if packager == "identity" {
		extension := filepath.Ext(artifactFilename)
		if strings.HasSuffix(strings.ToLower(artifactFilename), ".warc.zst") {
			extension = ".warc.zst"
		}
		return fmt.Sprintf("%s-%s%s", project, packageID[:24], extension)
	}
	return fmt.Sprintf("%s-%s.warc.zst", project, packageID[:24])
}

func makePackageDeliveryPlan(project, packageID, filename, manifestFilename string, createdAt time.Time, cfg deliveryConfig) (packageDeliveryPlan, error) {
	values := map[string]string{
		"{{PROJECT}}": project, "{{OBJECT_ID}}": packageID, "{{PACKAGE_ID}}": packageID,
		"{{FILENAME}}": filename, "{{PACKAGE_FILENAME}}": filename,
		"{{DATE}}": createdAt.UTC().Format("20060102150405"),
	}
	identifier := resolvePackageTemplate(cfg.Identifier, values)
	remoteName := resolvePackageTemplate(cfg.RemoteName, values)
	if err := validateRemoteName(remoteName); err != nil {
		return packageDeliveryPlan{}, err
	}
	metadata := make(map[string][]string, len(cfg.Metadata))
	for key, input := range cfg.Metadata {
		output := make([]string, len(input))
		for i, value := range input {
			output[i] = resolvePackageTemplate(value, values)
		}
		metadata[key] = output
	}
	return packageDeliveryPlan{
		Version: 1, Sink: cfg.Sink, CredentialsFile: cfg.CredentialsFile,
		Identifier: identifier, Metadata: metadata, RetentionNanos: int64(cfg.localArtifactRetention),
		Files: map[string]string{remoteName: filename, remoteName + ".manifest.jsonl": manifestFilename},
	}, nil
}

func resolvePackageTemplate(value string, replacements map[string]string) string {
	return strings.NewReplacer(
		"{{PROJECT}}", replacements["{{PROJECT}}"],
		"{{OBJECT_ID}}", replacements["{{OBJECT_ID}}"],
		"{{PACKAGE_ID}}", replacements["{{PACKAGE_ID}}"],
		"{{FILENAME}}", replacements["{{FILENAME}}"],
		"{{PACKAGE_FILENAME}}", replacements["{{PACKAGE_FILENAME}}"],
		"{{DATE}}", replacements["{{DATE}}"],
	).Replace(value)
}

func validateRemoteName(name string) error {
	if name == "" || name == "." || strings.Contains(name, `\`) || strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || filepath.ToSlash(filepath.Clean(name)) != name || strings.HasPrefix(name, "../") {
		return fmt.Errorf("resolved remote name %q is invalid", name)
	}
	return nil
}

const packageColumns = `package_id,project,packager,filename,manifest_filename,state,size_bytes,checksum,manifest_checksum,member_count,build_attempts,next_build_at,build_error,created_at,updated_at,sealed_at,purge_after,next_purge_attempt_at,purge_error,purged_at`

func scanPackage(row rowScanner) (packageRecord, error) {
	var pkg packageRecord
	err := row.Scan(&pkg.PackageID, &pkg.Project, &pkg.Packager, &pkg.Filename, &pkg.ManifestFilename, &pkg.State, &pkg.SizeBytes, &pkg.Checksum, &pkg.ManifestChecksum, &pkg.MemberCount, &pkg.BuildAttempts, &pkg.NextBuildAt, &pkg.BuildError, &pkg.CreatedAt, &pkg.UpdatedAt, &pkg.SealedAt, &pkg.PurgeAfter, &pkg.NextPurgeAt, &pkg.PurgeError, &pkg.PurgedAt)
	return pkg, err
}

func (s *deliveryStore) packageMembers(ctx context.Context, packageID string) ([]packageMember, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT m.package_id,m.ordinal,m.object_id,m.offset_bytes,m.size_bytes,m.checksum,a.project,a.filename,a.accepted_at FROM package_members m JOIN artifacts a ON a.object_id=m.object_id WHERE m.package_id=? ORDER BY m.ordinal`, packageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []packageMember
	for rows.Next() {
		var member packageMember
		if err := rows.Scan(&member.PackageID, &member.Ordinal, &member.ObjectID, &member.Offset, &member.SizeBytes, &member.Checksum, &member.Project, &member.Filename, &member.AcceptedAt); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

func (s *deliveryStore) sealPackage(ctx context.Context, packageID string, size int64, checksum, manifestChecksum string, now int64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE packages SET state='sealed',size_bytes=?,checksum=?,manifest_checksum=?,build_error=NULL,updated_at=?,sealed_at=? WHERE package_id=? AND state='building'`, size, checksum, manifestChecksum, now, now, packageID)
	return exactlyOne(result, err)
}

func (s *deliveryStore) markPackageBuildRetry(ctx context.Context, packageID, message string, retryAt, now int64) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE packages SET next_build_at=?,build_error=?,updated_at=? WHERE package_id=? AND state='building'`, retryAt, message, now, packageID)
	return exactlyOne(result, err)
}

func (s *deliveryStore) isolateInvalidMembers(ctx context.Context, packageID string, invalid map[string]string, now int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var messages []string
	for objectID, message := range invalid {
		if len(message) > 4096 {
			message = message[:4096]
		}
		messages = append(messages, objectID+": "+message)
		if _, err := tx.ExecContext(ctx, `UPDATE artifacts SET packaging_error=? WHERE object_id=? AND package_id=?`, message, objectID, packageID); err != nil {
			return err
		}
	}
	sort.Strings(messages)
	if _, err := tx.ExecContext(ctx, `DELETE FROM package_members WHERE package_id=? AND object_id NOT IN (SELECT object_id FROM artifacts WHERE package_id=? AND packaging_error IS NOT NULL)`, packageID, packageID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifacts SET package_id=NULL WHERE package_id=? AND packaging_error IS NULL`, packageID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE packages SET state='blocked',member_count=?,build_error=?,updated_at=? WHERE package_id=? AND state='building'`, len(invalid), strings.Join(messages, "; "), now, packageID); err != nil {
		return err
	}
	return tx.Commit()
}

func (w *deliveryWorker) runPackageBuildCycle(ctx context.Context) (bool, error) {
	pkg, ok, err := w.store.nextBuildingPackage(ctx, w.now().Unix())
	if err != nil {
		return false, err
	}
	if !ok {
		projects := make([]string, 0, len(w.cfg.Projects))
		for project := range w.cfg.Projects {
			projects = append(projects, project)
		}
		sort.Strings(projects)
		for _, project := range projects {
			projectCfg := w.cfg.Projects[project]
			pkg, ok, err = w.store.reservePackage(ctx, project, projectCfg.Packaging, projectCfg.Delivery, w.now())
			if err != nil || ok {
				break
			}
		}
	}
	if err != nil || !ok {
		return false, err
	}
	if err := w.materializePackage(ctx, pkg); err != nil {
		invalid, isolateErr := w.findInvalidPackageMembers(ctx, pkg)
		if isolateErr != nil {
			return true, isolateErr
		}
		if len(invalid) > 0 {
			if markErr := w.store.isolateInvalidMembers(ctx, pkg.PackageID, invalid, w.now().Unix()); markErr != nil {
				return true, errors.Join(err, markErr)
			}
			return true, nil
		}
		retryAt := w.now().Add(retryDelay(pkg.BuildAttempts)).Unix()
		if markErr := w.store.markPackageBuildRetry(ctx, pkg.PackageID, err.Error(), retryAt, w.now().Unix()); markErr != nil {
			return true, errors.Join(err, markErr)
		}
		return true, nil
	}
	return true, nil
}

func (w *deliveryWorker) findInvalidPackageMembers(ctx context.Context, pkg packageRecord) (map[string]string, error) {
	if pkg.Packager == "identity" {
		return nil, nil
	}
	members, err := w.store.packageMembers(ctx, pkg.PackageID)
	if err != nil {
		return nil, err
	}
	invalid := make(map[string]string)
	for _, member := range members {
		_, err := mergewarc.Merge(ctx, io.Discard, []mergewarc.Input{w.mergeInput(member)}, mergewarc.Options{})
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
			return nil, nil
		}
		invalid[member.ObjectID] = err.Error()
	}
	return invalid, nil
}

func (w *deliveryWorker) mergeInput(member packageMember) mergewarc.Input {
	path := filepath.Join(w.uploadsDir, member.ObjectID)
	return mergewarc.Input{
		Name: member.ObjectID + "/" + member.Filename,
		Open: func() (io.ReadCloser, error) { return os.Open(path) },
		Metadata: map[string]string{
			"object_id": member.ObjectID, "receipt_id": "receipt:" + member.ObjectID,
			"project": member.Project, "accepted_at": strconv.FormatInt(member.AcceptedAt, 10),
		},
	}
}

func (w *deliveryWorker) materializePackage(ctx context.Context, pkg packageRecord) error {
	members, err := w.store.packageMembers(ctx, pkg.PackageID)
	if err != nil {
		return err
	}
	if len(members) != pkg.MemberCount {
		return fmt.Errorf("package %s has %d members, expected %d", pkg.PackageID, len(members), pkg.MemberCount)
	}
	packageDir := filepath.Join(w.cfg.DataDir, "packages")
	manifestDir := filepath.Join(w.cfg.DataDir, "package-manifests")
	for _, dir := range []string{packageDir, manifestDir} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	if pkg.Packager == "identity" {
		return w.materializeIdentityPackage(ctx, pkg, members, packageDir, manifestDir)
	}
	if pkg.Packager != "mergewarc" {
		return fmt.Errorf("unsupported packager %q", pkg.Packager)
	}
	output, err := os.CreateTemp(packageDir, ".package-*")
	if err != nil {
		return err
	}
	outputTmp := output.Name()
	defer os.Remove(outputTmp)
	inputs := make([]mergewarc.Input, 0, len(members))
	for _, member := range members {
		inputs = append(inputs, w.mergeInput(member))
	}
	manifest, mergeErr := mergewarc.Merge(ctx, output, inputs, mergewarc.Options{})
	if mergeErr != nil {
		output.Close()
		return mergeErr
	}
	if err := output.Sync(); err != nil {
		output.Close()
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	manifestFile, err := os.CreateTemp(manifestDir, ".manifest-*")
	if err != nil {
		return err
	}
	manifestTmp := manifestFile.Name()
	defer os.Remove(manifestTmp)
	manifestHash := blake3.New()
	if err := mergewarc.WriteJSONL(io.MultiWriter(manifestFile, manifestHash), manifest); err != nil {
		manifestFile.Close()
		return err
	}
	if err := manifestFile.Sync(); err != nil {
		manifestFile.Close()
		return err
	}
	if err := manifestFile.Close(); err != nil {
		return err
	}
	packagePath := filepath.Join(packageDir, pkg.Filename)
	manifestPath := filepath.Join(manifestDir, pkg.ManifestFilename)
	if err := os.Rename(outputTmp, packagePath); err != nil {
		return err
	}
	if err := syncDirectory(packageDir); err != nil {
		return err
	}
	if err := os.Rename(manifestTmp, manifestPath); err != nil {
		return err
	}
	if err := syncDirectory(manifestDir); err != nil {
		return err
	}
	packageChecksum, err := checksumForAlgorithm(manifest.Output.Checksums, mergewarc.ChecksumBLAKE3)
	if err != nil {
		return err
	}
	manifestChecksum := "blake3:" + hex.EncodeToString(manifestHash.Sum(nil))
	if err := w.store.sealPackage(ctx, pkg.PackageID, manifest.Output.Size, packageChecksum, manifestChecksum, w.now().Unix()); err != nil {
		return err
	}
	return nil
}

func (w *deliveryWorker) materializeIdentityPackage(ctx context.Context, pkg packageRecord, members []packageMember, packageDir, manifestDir string) error {
	if len(members) != 1 {
		return fmt.Errorf("identity package %s has %d members", pkg.PackageID, len(members))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	member := members[0]
	placeholder, err := os.CreateTemp(packageDir, ".package-*")
	if err != nil {
		return err
	}
	outputTmp := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		return err
	}
	if err := os.Remove(outputTmp); err != nil {
		return err
	}
	defer os.Remove(outputTmp)
	if err := os.Link(filepath.Join(w.uploadsDir, member.ObjectID), outputTmp); err != nil {
		return fmt.Errorf("link identity package: %w", err)
	}
	linked, err := os.Open(outputTmp)
	if err != nil {
		return err
	}
	if err := linked.Sync(); err != nil {
		linked.Close()
		return err
	}
	if err := linked.Close(); err != nil {
		return err
	}
	manifestFile, err := os.CreateTemp(manifestDir, ".manifest-*")
	if err != nil {
		return err
	}
	manifestTmp := manifestFile.Name()
	defer os.Remove(manifestTmp)
	manifestHash := blake3.New()
	encoder := json.NewEncoder(io.MultiWriter(manifestFile, manifestHash))
	lines := []any{
		map[string]any{"type": "canner-package", "version": 1, "packager": "identity", "format": "artifact"},
		map[string]any{"type": "input", "name": member.ObjectID + "/" + member.Filename, "object_id": member.ObjectID, "receipt_id": "receipt:" + member.ObjectID, "offset": 0, "size": member.SizeBytes, "checksums": []string{member.Checksum}, "accepted_at": member.AcceptedAt},
		map[string]any{"type": "output", "size": member.SizeBytes, "checksums": []string{member.Checksum}, "inputs": 1},
	}
	for _, line := range lines {
		if err := encoder.Encode(line); err != nil {
			manifestFile.Close()
			return err
		}
	}
	if err := manifestFile.Sync(); err != nil {
		manifestFile.Close()
		return err
	}
	if err := manifestFile.Close(); err != nil {
		return err
	}
	packagePath := filepath.Join(packageDir, pkg.Filename)
	manifestPath := filepath.Join(manifestDir, pkg.ManifestFilename)
	if err := os.Rename(outputTmp, packagePath); err != nil {
		return err
	}
	if err := syncDirectory(packageDir); err != nil {
		return err
	}
	if err := os.Rename(manifestTmp, manifestPath); err != nil {
		return err
	}
	if err := syncDirectory(manifestDir); err != nil {
		return err
	}
	return w.store.sealPackage(ctx, pkg.PackageID, member.SizeBytes, member.Checksum, "blake3:"+hex.EncodeToString(manifestHash.Sum(nil)), w.now().Unix())
}

func checksumForAlgorithm(checksums []string, algorithm mergewarc.ChecksumAlgorithm) (string, error) {
	prefix := string(algorithm) + ":"
	for _, checksum := range checksums {
		if strings.HasPrefix(checksum, prefix) {
			return checksum, nil
		}
	}
	return "", fmt.Errorf("package output has no %s checksum", algorithm)
}

func (s *deliveryStore) nextSourceToPurge(ctx context.Context, now int64) (artifactRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT a.object_id,a.project,a.filename,a.checksum,a.size_bytes,a.accepted_at,a.package_id,a.packaging_error,a.source_purged_at,a.next_source_purge_at,a.source_purge_error FROM artifacts a JOIN packages p ON p.package_id=a.package_id WHERE p.state='sealed' AND a.source_purged_at IS NULL AND a.next_source_purge_at<=? ORDER BY a.next_source_purge_at,p.created_at,a.accepted_at,a.object_id LIMIT 1`, now)
	var item artifactRecord
	err := row.Scan(&item.ObjectID, &item.Project, &item.Filename, &item.Checksum, &item.SizeBytes, &item.AcceptedAt, &item.PackageID, &item.PackagingError, &item.SourcePurgedAt, &item.NextSourcePurge, &item.SourcePurgeError)
	if errors.Is(err, sql.ErrNoRows) {
		return artifactRecord{}, false, nil
	}
	return item, err == nil, err
}

func (s *deliveryStore) markSourcePurged(ctx context.Context, objectID string, now int64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE artifacts SET source_purged_at=?,source_purge_error=NULL WHERE object_id=? AND source_purged_at IS NULL`, now, objectID)
	return exactlyOne(result, err)
}

func (s *deliveryStore) markSourcePurgeRetry(ctx context.Context, objectID, message string, retryAt int64) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE artifacts SET next_source_purge_at=?,source_purge_error=? WHERE object_id=? AND source_purged_at IS NULL`, retryAt, message, objectID)
	return exactlyOne(result, err)
}

func (w *deliveryWorker) runPackagedSourcePurgeCycle(ctx context.Context) (bool, error) {
	item, ok, err := w.store.nextSourceToPurge(ctx, w.now().Unix())
	if err != nil || !ok {
		return false, err
	}
	if err := purgeUpload(ctx, w.uploadsDir, item.ObjectID); err != nil {
		if errors.Is(err, context.Canceled) {
			return true, err
		}
		return true, w.store.markSourcePurgeRetry(ctx, item.ObjectID, err.Error(), unixCeil(w.now().Add(time.Minute)))
	}
	return true, w.store.markSourcePurged(ctx, item.ObjectID, w.now().Unix())
}

const deliveryColumns = `package_id,sink_id,state,plan,attempts,next_attempt_at,last_error,remote_id,updated_at,delivered_at`

func scanDelivery(row rowScanner) (deliveryRecord, error) {
	var delivery deliveryRecord
	err := row.Scan(&delivery.PackageID, &delivery.SinkID, &delivery.State, &delivery.Plan, &delivery.Attempts, &delivery.NextAttempt, &delivery.LastError, &delivery.RemoteID, &delivery.UpdatedAt, &delivery.DeliveredAt)
	return delivery, err
}

func (s *deliveryStore) claimPackageDelivery(ctx context.Context, now int64) (deliveryRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `UPDATE deliveries SET state='delivering',attempts=attempts+1,updated_at=? WHERE (package_id,sink_id)=(SELECT d.package_id,d.sink_id FROM deliveries d JOIN packages p ON p.package_id=d.package_id WHERE p.state='sealed' AND d.state IN ('pending','retry_wait') AND d.next_attempt_at<=? ORDER BY p.created_at,d.package_id,d.sink_id LIMIT 1) RETURNING `+deliveryColumns, now, now)
	delivery, err := scanDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return deliveryRecord{}, false, nil
	}
	return delivery, err == nil, err
}

func (s *deliveryStore) markPackageDelivered(ctx context.Context, packageID, sinkID, remoteID string, deliveredAt, purgeAfter int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE deliveries SET state='delivered',next_attempt_at=0,remote_id=?,last_error=NULL,updated_at=?,delivered_at=? WHERE package_id=? AND sink_id=? AND state='delivering'`, remoteID, deliveredAt, deliveredAt, packageID, sinkID)
	if err := exactlyOne(result, err); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE packages SET purge_after=CASE WHEN purge_after IS NULL OR purge_after<? THEN ? ELSE purge_after END,next_purge_attempt_at=CASE WHEN next_purge_attempt_at IS NULL OR next_purge_attempt_at<? THEN ? ELSE next_purge_attempt_at END,updated_at=? WHERE package_id=?`, purgeAfter, purgeAfter, purgeAfter, purgeAfter, deliveredAt, packageID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *deliveryStore) markPackageRetry(ctx context.Context, packageID, sinkID, message string, retryAt, now int64) error {
	if len(message) > 4096 {
		message = message[:4096]
	}
	result, err := s.db.ExecContext(ctx, `UPDATE deliveries SET state='retry_wait',next_attempt_at=?,last_error=?,updated_at=? WHERE package_id=? AND sink_id=? AND state='delivering'`, retryAt, message, now, packageID, sinkID)
	return exactlyOne(result, err)
}

func (w *deliveryWorker) runPackageDeliveryCycle(ctx context.Context) (bool, error) {
	delivery, ok, err := w.store.claimPackageDelivery(ctx, w.now().Unix())
	if err != nil || !ok {
		return false, err
	}
	var plan packageDeliveryPlan
	err = json.Unmarshal([]byte(delivery.Plan), &plan)
	if err == nil {
		err = w.deliverPackage(ctx, plan, w.cfg.DataDir)
	}
	if err == nil {
		deliveredAt := w.now()
		purgeAfter := unixCeil(deliveredAt.Add(time.Duration(plan.RetentionNanos)))
		if err := w.store.markPackageDelivered(ctx, delivery.PackageID, delivery.SinkID, plan.Identifier, deliveredAt.Unix(), purgeAfter); err != nil {
			return true, err
		}
		return true, nil
	}
	retryAt := w.now().Add(retryDelay(delivery.Attempts)).Unix()
	return true, w.store.markPackageRetry(ctx, delivery.PackageID, delivery.SinkID, err.Error(), retryAt, w.now().Unix())
}

func (s *deliveryStore) nextPackagePurge(ctx context.Context, now int64) (packageRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+packageColumns+` FROM packages p WHERE state='sealed' AND purged_at IS NULL AND next_purge_attempt_at<=? AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.package_id=p.package_id AND d.state!='delivered') ORDER BY next_purge_attempt_at,package_id LIMIT 1`, now)
	pkg, err := scanPackage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return packageRecord{}, false, nil
	}
	return pkg, err == nil, err
}

func (s *deliveryStore) markPackagePurged(ctx context.Context, packageID string, now int64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE packages SET purged_at=?,next_purge_attempt_at=NULL,purge_error=NULL,updated_at=? WHERE package_id=? AND state='sealed' AND purged_at IS NULL`, now, now, packageID)
	return exactlyOne(result, err)
}

func (s *deliveryStore) markPackagePurgeRetry(ctx context.Context, packageID, message string, retryAt, now int64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE packages SET next_purge_attempt_at=?,purge_error=?,updated_at=? WHERE package_id=? AND state='sealed' AND purged_at IS NULL`, retryAt, message, now, packageID)
	return exactlyOne(result, err)
}

func (w *deliveryWorker) runPackagePurgeCycle(ctx context.Context) (bool, error) {
	pkg, ok, err := w.store.nextPackagePurge(ctx, w.now().Unix())
	if err != nil || !ok {
		return false, err
	}
	var removeErr error
	for _, path := range []string{filepath.Join(w.cfg.DataDir, "packages", pkg.Filename), filepath.Join(w.cfg.DataDir, "package-manifests", pkg.ManifestFilename)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	if removeErr != nil {
		retryAt := unixCeil(w.now().Add(time.Minute))
		return true, w.store.markPackagePurgeRetry(ctx, pkg.PackageID, removeErr.Error(), retryAt, w.now().Unix())
	}
	for _, dir := range []string{filepath.Join(w.cfg.DataDir, "packages"), filepath.Join(w.cfg.DataDir, "package-manifests")} {
		if err := syncDirectory(dir); err != nil {
			removeErr = errors.Join(removeErr, err)
		}
	}
	if removeErr != nil {
		retryAt := unixCeil(w.now().Add(time.Minute))
		return true, w.store.markPackagePurgeRetry(ctx, pkg.PackageID, removeErr.Error(), retryAt, w.now().Unix())
	}
	return true, w.store.markPackagePurged(ctx, pkg.PackageID, w.now().Unix())
}

func (s *deliveryStore) listPackages(ctx context.Context) ([]packageRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+packageColumns+` FROM packages ORDER BY created_at,package_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var packages []packageRecord
	for rows.Next() {
		pkg, err := scanPackage(rows)
		if err != nil {
			return nil, err
		}
		packages = append(packages, pkg)
	}
	return packages, rows.Err()
}

func (s *deliveryStore) listDeliveries(ctx context.Context) ([]deliveryRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries ORDER BY package_id,sink_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deliveries []deliveryRecord
	for rows.Next() {
		delivery, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

func printPackages(ctx context.Context, cfg runtimeConfig, output io.Writer) error {
	store, err := openDeliveryStore(cfg.DataDir)
	if err != nil {
		return err
	}
	defer store.close()
	packages, err := store.listPackages(ctx)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	for _, pkg := range packages {
		if err := encoder.Encode(struct {
			Type    string        `json:"type"`
			Package packageRecord `json:"package"`
		}{Type: "package", Package: pkg}); err != nil {
			return err
		}
	}
	deliveries, err := store.listDeliveries(ctx)
	if err != nil {
		return err
	}
	for _, delivery := range deliveries {
		if err := encoder.Encode(struct {
			Type     string         `json:"type"`
			Delivery deliveryRecord `json:"delivery"`
		}{Type: "delivery", Delivery: delivery}); err != nil {
			return err
		}
	}
	return nil
}
