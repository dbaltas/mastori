package datasets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithy "github.com/aws/smithy-go"
)

// s3Store is the ObjectStore backed by an S3 bucket. On EKS it authenticates
// through IRSA (the SDK picks up the injected AWS_ROLE_ARN /
// AWS_WEB_IDENTITY_TOKEN_FILE automatically); Endpoint may point at a local
// S3-compatible service (MinIO) for the integration tests.
type s3Store struct {
	client *awss3.Client
	bucket string
}

// NewS3Store builds a store from the environment: the bucket comes from the
// caller (DATASETS_S3_BUCKET; empty disables the remote cache), the default
// AWS config chain supplies region and credentials, and
// DATASETS_S3_ENDPOINT overrides the endpoint (MinIO in the integration
// tests).
func NewS3Store(ctx context.Context, bucket string) (*s3Store, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	s := &s3Store{bucket: bucket}
	opts := []func(*awss3.Options){}
	if endpoint := os.Getenv("DATASETS_S3_ENDPOINT"); endpoint != "" {
		// Non-AWS S3-compatible endpoints (MinIO/LocalStack in the
		// integration tests): path-style addressing; virtual-host style
		// (bucket.localhost) does not resolve or parse.
		opts = append(opts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}
	s.client = awss3.NewFromConfig(cfg, opts...)
	return s, nil
}

// isNotFound reports whether the error is an AWS API error whose code marks a
// missing object (HeadObject NotFound / GetObject NoSuchKey).
//
// NoSuchBucket is deliberately NOT a miss: a wrong or missing bucket is a
// configuration error, and silently re-seeding every dataset into a bucket
// that cannot be read would waste the seeding on every boot. It surfaces as
// a hard error instead.
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return true
		}
	}
	return false
}

func (s *s3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *s3Store) Get(ctx context.Context, key, dstPath string) error {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	return writeBodyToFile(out.Body, dstPath)
}

func (s *s3Store) Put(ctx context.Context, key, srcPath string) error {
	f, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", srcPath, err)
	}
	defer f.Close()
	_, err = s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	return err
}

// writeBodyToFile streams the body into dstPath through a temp file and a
// rename, so a concurrent reader never observes a half-written object.
func writeBodyToFile(body io.Reader, dstPath string) error {
	tmp, err := os.CreateTemp(filepath.Dir(dstPath), ".tmp-"+filepath.Base(dstPath)+"-")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := io.Copy(tmp, body); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		return fmt.Errorf("rename %s: %w", tmpPath, err)
	}
	return nil
}
