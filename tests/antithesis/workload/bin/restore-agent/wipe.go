package main

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
)

// s3Wiper deletes a namespace's objects through the ledger's own backup
// storage (built with -tags s3 in the image; without the tag every call
// fails, which the driver treats as "no chained restore this cycle").
type s3Wiper struct {
	cfg config
}

func newS3Wiper(cfg config) wiper {
	return &s3Wiper{cfg: cfg}
}

func (s *s3Wiper) wipe(ctx context.Context, bucketID string) (int, error) {
	st, err := backup.NewStorage(backup.StorageConfig{
		Driver:            "s3",
		S3Bucket:          s.cfg.s3Bucket,
		S3Region:          s.cfg.s3Region,
		S3Endpoint:        s.cfg.s3Endpoint,
		S3AccessKeyID:     s.cfg.s3AccessKey,
		S3SecretAccessKey: s.cfg.s3SecretKey,
	})
	if err != nil {
		return 0, err
	}
	keys, err := st.ListFiles(ctx, bucketID+"/")
	if err != nil {
		return 0, fmt.Errorf("listing %s/: %w", bucketID, err)
	}
	n := 0
	for _, k := range keys {
		if err := st.DeleteFile(ctx, k); err != nil {
			return n, fmt.Errorf("deleting %s: %w", k, err)
		}
		n++
	}

	return n, nil
}
