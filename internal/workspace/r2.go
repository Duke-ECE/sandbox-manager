// Package workspace contains the object-store control plane for session files.
// Archive bytes travel between a sandbox and R2, never through this process.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const MaxURLLifetime = 10 * time.Minute

type R2Config struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
}

// ObjectInfo is the metadata verified after a sandbox uploads an archive.
type ObjectInfo struct {
	Size   int64
	SHA256 string
}

type ListedObject struct {
	Key          string
	LastModified time.Time
}

type UploadURL struct {
	URL     string
	Headers map[string]string
}

type R2 struct {
	bucket  string
	client  *s3.Client
	presign *s3.PresignClient
}

func NewR2(cfg R2Config) (*R2, error) {
	if cfg.Bucket == "" || cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("R2 bucket and access keys are required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return nil, errors.New("R2 endpoint must be an HTTPS origin")
	}
	if !strings.HasSuffix(u.Hostname(), ".r2.cloudflarestorage.com") {
		return nil, errors.New("R2 endpoint must use the Cloudflare S3 API domain")
	}
	client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(cfg.Endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		UsePathStyle: true,
	})
	return &R2{bucket: cfg.Bucket, client: client, presign: s3.NewPresignClient(client)}, nil
}

func validObjectKey(key string) bool {
	if !strings.HasPrefix(key, "sessions/") || strings.Contains(key, "..") || strings.ContainsAny(key, "\\\r\n") {
		return false
	}
	parts := strings.Split(key, "/")
	return len(parts) == 4 && parts[0] == "sessions" && parts[2] == "revisions" && parts[1] != "" && strings.HasSuffix(parts[3], ".tar.gz")
}

func validLifetime(ttl time.Duration) bool { return ttl >= time.Second && ttl <= MaxURLLifetime }

func (r *R2) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if !validObjectKey(key) || !validLifetime(ttl) {
		return "", errors.New("invalid object key or URL lifetime")
	}
	result, err := r.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &r.bucket, Key: &key}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign R2 GET: %w", err)
	}
	return result.URL, nil
}

// PresignPut binds the request to one object, its MD5 and its claimed SHA-256.
// R2 validates Content-MD5 against the actual uploaded bytes. The SHA-256 is
// checked again when restoring the archive.
func (r *R2) PresignPut(ctx context.Context, key, md5Base64, sha256Hex string, ttl time.Duration) (UploadURL, error) {
	if !validObjectKey(key) || !validLifetime(ttl) || len(md5Base64) != 24 || len(sha256Hex) != 64 {
		return UploadURL{}, errors.New("invalid upload parameters")
	}
	result, err := r.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket, Key: &key, ContentMD5: &md5Base64,
		IfNoneMatch: aws.String("*"),
		Metadata:    map[string]string{"sha256": sha256Hex},
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return UploadURL{}, fmt.Errorf("presign R2 PUT: %w", err)
	}
	headers := make(map[string]string, len(result.SignedHeader))
	for name, values := range result.SignedHeader {
		if len(values) > 0 && !strings.EqualFold(name, "Host") {
			headers[name] = values[0]
		}
	}
	return UploadURL{URL: result.URL, Headers: headers}, nil
}

func (r *R2) Head(ctx context.Context, key string) (ObjectInfo, error) {
	if !validObjectKey(key) {
		return ObjectInfo{}, errors.New("invalid object key")
	}
	out, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &r.bucket, Key: &key})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("head R2 object: %w", err)
	}
	return ObjectInfo{Size: aws.ToInt64(out.ContentLength), SHA256: out.Metadata["sha256"]}, nil
}

func (r *R2) Delete(ctx context.Context, key string) error {
	if !validObjectKey(key) {
		return errors.New("invalid object key")
	}
	_, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &r.bucket, Key: &key})
	return err
}

func (r *R2) ListSession(ctx context.Context, sessionID string) ([]ListedObject, error) {
	if sessionID == "" || strings.ContainsAny(sessionID, "/\\.\r\n") {
		return nil, errors.New("invalid session id")
	}
	prefix := "sessions/" + sessionID + "/revisions/"
	pages := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{Bucket: &r.bucket, Prefix: &prefix})
	var result []ListedObject
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list R2 session objects: %w", err)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if strings.HasPrefix(key, prefix) && validObjectKey(key) {
				result = append(result, ListedObject{Key: key, LastModified: aws.ToTime(object.LastModified)})
			}
		}
	}
	return result, nil
}
