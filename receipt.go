package main

type artifactReceipt struct {
	ID         string `json:"id"`
	Issuer     string `json:"issuer"`
	ObjectID   string `json:"object_id"`
	Checksum   string `json:"checksum"`
	SizeBytes  int64  `json:"size_bytes"`
	AcceptedAt int64  `json:"accepted_at"`
}
