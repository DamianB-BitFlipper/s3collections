// s3.go provides a production S3-backed BlobStore built on AWS SDK for Go
// v2. Uploads stream through the s3manager uploader; downloads support byte
// ranges.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config configures an S3BlobStore.
type S3Config struct {
	// Bucket is the S3 bucket that holds the blobs. Required.
	Bucket string
	// Prefix is prepended to every blob key (e.g. "blobs/"). Optional.
	Prefix string
	// Region is the AWS region. Optional when Client is set or the
	// environment provides one.
	Region string
	// Endpoint overrides the S3 endpoint (for S3-compatible stores such as
	// MinIO or LocalStack). Optional.
	Endpoint string
	// AccessKeyID and SecretAccessKey provide static credentials. When
	// empty, the default AWS credential chain is used.
	AccessKeyID     string
	SecretAccessKey string
	// ForcePathStyle forces path-style addressing, required by many
	// S3-compatible stores.
	ForcePathStyle bool
	// PartSize is the multipart chunk size in bytes. <= 0 uses the
	// manager default.
	PartSize int64
	// Concurrency is the number of parallel part uploads. <= 0 uses the
	// manager default.
	Concurrency int
	// Client, when non-nil, is used directly and the credential/region
	// fields are ignored.
	Client *s3.Client
}

// S3BlobStore stores blobs in an S3 bucket.
type S3BlobStore struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	prefix   string
}

// NewS3BlobStore builds an S3BlobStore. When cfg.Client is nil it loads the
// default AWS configuration (honoring cfg.Region, cfg.Endpoint, and static
// credentials when set).
func NewS3BlobStore(ctx context.Context, cfg S3Config) (*S3BlobStore, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("storage: s3 bucket is required")
	}
	client := cfg.Client
	if client == nil {
		opts := []func(*awsconfig.LoadOptions) error{}
		if cfg.Region != "" {
			opts = append(opts, awsconfig.WithRegion(cfg.Region))
		}
		if cfg.AccessKeyID != "" {
			opts = append(opts, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("storage: load aws config: %w", err)
		}
		client = s3.NewFromConfig(awsCfg, s3ClientOptions(cfg)...)
	}
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		if cfg.PartSize > 0 {
			u.PartSize = cfg.PartSize
		}
		if cfg.Concurrency > 0 {
			u.Concurrency = cfg.Concurrency
		}
	})
	prefix := strings.TrimPrefix(cfg.Prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &S3BlobStore{
		client:   client,
		uploader: uploader,
		bucket:   cfg.Bucket,
		prefix:   prefix,
	}, nil
}

func s3ClientOptions(cfg S3Config) []func(*s3.Options) {
	var opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.ForcePathStyle {
		opts = append(opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	return opts
}

func (s *S3BlobStore) fullKey(key string) string { return s.prefix + strings.TrimPrefix(key, "/") }

// exactSizeReader validates a declared length without buffering the body. It
// probes one byte past the declared end so both short and long inputs fail.
type exactSizeReader struct {
	ctx       context.Context
	r         io.Reader
	remaining int64
	validated bool
}

func (r *exactSizeReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.validated {
		return 0, io.EOF
	}
	if r.remaining > 0 {
		if int64(len(p)) > r.remaining {
			p = p[:r.remaining]
		}
		n, err := r.r.Read(p)
		r.remaining -= int64(n)
		if err == io.EOF && r.remaining > 0 {
			return n, ErrSizeMismatch
		}
		if err != nil && err != io.EOF {
			return n, err
		}
		if n == 0 && err == io.EOF {
			return 0, ErrSizeMismatch
		}
		// Probe for trailing data on the next call even if this read reported
		// EOF together with the final bytes.
		return n, nil
	}
	var probe [1]byte
	n, err := r.r.Read(probe[:])
	if n != 0 {
		return 0, ErrSizeMismatch
	}
	if err == nil {
		return 0, nil
	}
	if err != io.EOF {
		return 0, err
	}
	r.validated = true
	return 0, io.EOF
}

// Put streams r into S3. The manager switches to multipart upload for large
// bodies and aborts a multipart upload when reading or uploading fails. Known
// sizes are validated in-flight and are never copied into one large buffer.
func (s *S3BlobStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if r == nil {
		return errors.New("storage: nil blob reader")
	}
	if size >= 0 {
		r = &exactSizeReader{ctx: ctx, r: r, remaining: size}
	}
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   r,
	})
	return mapS3Err(err)
}

func (s *S3BlobStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return nil, mapS3Err(err)
	}
	return out.Body, nil
}

// OpenRange returns the byte range [start, end) of the blob.
func (s *S3BlobStore) OpenRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	if start < 0 || end < start {
		return nil, fmt.Errorf("storage: invalid range [%d, %d)", start, end)
	}
	if start == end {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end-1)),
	})
	if err != nil {
		return nil, mapS3Err(err)
	}
	return out.Body, nil
}

func (s *S3BlobStore) Stat(ctx context.Context, key string) (BlobInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return BlobInfo{}, mapS3Err(err)
	}
	info := BlobInfo{Key: key}
	if out.ContentLength != nil {
		info.Size = *out.ContentLength
	}
	if out.ETag != nil {
		info.ETag = *out.ETag
	}
	return info, nil
}

// Delete removes a blob. S3 deletes are idempotent, so deleting a missing
// blob is not an error.
func (s *S3BlobStore) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	return mapS3Err(err)
}

// Close is a no-op; the S3 client has no persistent resources.
func (s *S3BlobStore) Close() error { return nil }

// mapS3Err translates AWS errors into storage sentinel errors.
func mapS3Err(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithyAPIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return errors.Join(ErrNotFound, err)
		}
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return errors.Join(ErrNotFound, err)
	}
	return err
}

// smithyAPIError is the subset of smithy.APIError used here, declared to
// keep imports small.
type smithyAPIError interface {
	ErrorCode() string
	ErrorMessage() string
	Error() string
}
