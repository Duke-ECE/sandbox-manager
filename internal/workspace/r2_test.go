package workspace

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testR2(t *testing.T) *R2 {
	t.Helper()
	r, err := NewR2(R2Config{
		Endpoint:        "https://0123456789abcdef.r2.cloudflarestorage.com",
		Bucket:          "workspaces",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPresignedURLsAreObjectScopedAndShortLived(t *testing.T) {
	r := testR2(t)
	key := "sessions/sess-123/revisions/rev-456.tar.gz"
	getURL, err := r.PresignGet(context.Background(), key, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	put, err := r.PresignPut(context.Background(), key, "1B2M2Y8AsgTpgAmY7PhCfg==", strings.Repeat("a", 64), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{getURL, put.URL} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if u.Scheme != "https" || u.Host != "0123456789abcdef.r2.cloudflarestorage.com" || u.Path != "/workspaces/"+key {
			t.Fatalf("unexpected signed URL scope: %s", u.Redacted())
		}
		if got := u.Query().Get("X-Amz-Expires"); got != "300" {
			t.Fatalf("expiry = %q, want 300", got)
		}
		if u.Query().Get("X-Amz-Signature") == "" {
			t.Fatal("missing signature")
		}
	}
	if put.Headers["Content-Md5"] != "1B2M2Y8AsgTpgAmY7PhCfg==" {
		t.Fatalf("MD5 must be a signed upload header: %#v", put.Headers)
	}
	if put.Headers["X-Amz-Meta-Sha256"] != strings.Repeat("a", 64) {
		t.Fatalf("SHA-256 must be a signed upload header: %#v", put.Headers)
	}
	if put.Headers["If-None-Match"] != "*" {
		t.Fatalf("upload must reject an existing key: %#v", put.Headers)
	}
}

func TestRejectInvalidR2ConfigAndObjectScope(t *testing.T) {
	_, err := NewR2(R2Config{Endpoint: "http://example.com", Bucket: "x", AccessKeyID: "a", SecretAccessKey: "b"})
	if err == nil {
		t.Fatal("insecure endpoint accepted")
	}
	r := testR2(t)
	for _, key := range []string{"other/object.tar.gz", "sessions/../revisions/a.tar.gz", "sessions/s/revisions/a.tar.gz\n"} {
		if _, err := r.PresignGet(context.Background(), key, time.Minute); err == nil {
			t.Fatalf("invalid key accepted: %q", key)
		}
	}
	if _, err := r.PresignGet(context.Background(), "sessions/s/revisions/a.tar.gz", time.Hour); err == nil {
		t.Fatal("long-lived URL accepted")
	}
}
