package workspace

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// TestR2LiveRoundTrip is opt-in. It proves Cloudflare accepts our exact
// signed PUT headers and that HEAD/GET see the uploaded object.
func TestR2LiveRoundTrip(t *testing.T) {
	keys := []string{"R2_ENDPOINT", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"}
	for _, key := range keys {
		if os.Getenv(key) == "" {
			t.Skip("R2 live environment is not configured")
		}
	}
	r, err := NewR2(R2Config{
		Endpoint: os.Getenv("R2_ENDPOINT"), Bucket: os.Getenv("R2_BUCKET"),
		AccessKeyID: os.Getenv("R2_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("R2_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	key := "sessions/r2-integration-test/revisions/" + hex.EncodeToString(random) + ".tar.gz"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := r.Delete(cleanupCtx, key); err != nil {
			t.Errorf("delete test object: %v", err)
		}
	})
	body := []byte("PolyAgent direct R2 transfer test\n")
	md5Sum, sha := md5.Sum(body), sha256.Sum256(body)
	shaHex := hex.EncodeToString(sha[:])
	put, err := r.PresignPut(ctx, key, base64.StdEncoding.EncodeToString(md5Sum[:]), shaHex, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, put.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range put.Headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signed PUT status = %d", resp.StatusCode)
	}
	replay, err := http.NewRequestWithContext(ctx, http.MethodPut, put.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range put.Headers {
		replay.Header.Set(name, value)
	}
	replayed, err := http.DefaultClient.Do(replay)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, io.LimitReader(replayed.Body, 1024))
	replayed.Body.Close()
	if replayed.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("signed PUT replay status = %d", replayed.StatusCode)
	}
	info, err := r.Head(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(body)) || info.SHA256 != shaHex {
		t.Fatalf("R2 HEAD mismatch: %+v", info)
	}
	getURL, err := r.PresignGet(ctx, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()
	got, err := io.ReadAll(io.LimitReader(getResp.Body, 1024))
	if err != nil {
		t.Fatal(err)
	}
	if getResp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("signed GET failed: status=%d", getResp.StatusCode)
	}
}
