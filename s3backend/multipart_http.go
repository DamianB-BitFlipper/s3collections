package s3backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

const portableMultipartPartSize int64 = 64 << 20

type initiateMPUResult struct {
	UploadID string `xml:"UploadId"`
}
type completeMPUResult struct {
	ETag string `xml:"ETag"`
}
type completeMPURequest struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []completeMPUPart `xml:"Part"`
}
type completeMPUPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// PutMultipart implements the standard S3 multipart protocol used by AWS S3,
// R2, MinIO, and compatible services. It never uses append/write-offset APIs.
// Multipart completion preconditions are not uniformly supported, so strong
// conditional multipart writes are deliberately rejected.
func (c *HTTPClient) PutMultipart(ctx context.Context, key string, r io.Reader, size int64, pre *Preconditions) (err error) {
	if err = validateObjectKey("PutMultipart", key); err != nil {
		return err
	}
	if r == nil || size <= 0 {
		return &Error{Op: "PutMultipart", Key: key, Code: "InvalidArgument", Message: "invalid reader or size"}
	}
	if size > portableMultipartPartSize*10000 {
		return &Error{Op: "PutMultipart", Key: key, Code: "TooManyParts", Message: "object exceeds 10000 portable parts"}
	}
	if pre != nil && (pre.IfNoneMatch || pre.IfMatchETag != "") {
		return &Error{Op: "PutMultipart", Key: key, Code: "UnsupportedPrecondition", Message: "conditional multipart is not portable"}
	}
	uploadID, err := c.createMPU(ctx, key)
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if abortErr := c.abortMPU(abortCtx, key, uploadID); abortErr != nil {
				err = errors.Join(err, fmt.Errorf("abort multipart upload: %w", abortErr))
			}
		}
	}()
	parts := make([]completeMPUPart, 0, (size+portableMultipartPartSize-1)/portableMultipartPartSize)
	remaining := size
	for part := 1; remaining > 0; part++ {
		if part > 10000 {
			return &Error{Op: "PutMultipart", Key: key, Code: "TooManyParts", Message: "more than 10000 parts"}
		}
		n := portableMultipartPartSize
		if remaining < n {
			n = remaining
		}
		etag, e := c.uploadMPUPart(ctx, key, uploadID, part, io.LimitReader(r, n), n)
		if e != nil {
			return e
		}
		parts = append(parts, completeMPUPart{PartNumber: part, ETag: quoteETag(etag)})
		remaining -= n
	}
	extra, readErr := io.ReadAll(io.LimitReader(r, 1))
	if readErr != nil || len(extra) != 0 {
		return &Error{Op: "PutMultipart", Key: key, Code: "InvalidArgument", Message: "reader contains bytes beyond declared size"}
	}
	_, err = c.completeMPU(ctx, key, uploadID, parts)
	if err == nil {
		completed = true
	}
	return err
}
func (c *HTTPClient) createMPU(ctx context.Context, key string) (string, error) {
	q := url.Values{"uploads": {""}}
	req, err := c.newRequest(ctx, http.MethodPost, c.keyURL(key), nil, nil, q)
	if err != nil {
		return "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", transportError(ctx, "CreateMultipartUpload", key, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", decodeS3Error("CreateMultipartUpload", key, resp)
	}
	var out initiateMPUResult
	if err = xml.NewDecoder(resp.Body).Decode(&out); err != nil || out.UploadID == "" {
		return "", &Error{Op: "CreateMultipartUpload", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "invalid initiate response"}
	}
	return out.UploadID, nil
}
func (c *HTTPClient) uploadMPUPart(ctx context.Context, key, id string, part int, r io.Reader, size int64) (string, error) {
	f, err := os.CreateTemp("", "s3backend-part-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	defer func() { f.Close(); os.Remove(name) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		return "", err
	}
	if n != size {
		return "", io.ErrUnexpectedEOF
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	q := url.Values{"partNumber": {strconv.Itoa(part)}, "uploadId": {id}}
	u := c.keyURL(key)
	u.RawQuery = sigV4EncodeQuery(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), f)
	if err != nil {
		return "", err
	}
	req.ContentLength = size
	ph := hex.EncodeToString(h.Sum(nil))
	req.Header.Set("X-Amz-Content-Sha256", ph)
	c.signer.sign(req, ph, time.Now())
	resp, err := c.client.Do(req)
	if err != nil {
		return "", transportError(ctx, "UploadPart", key, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", decodeS3Error("UploadPart", key, resp)
	}
	etag := unquoteETag(resp.Header.Get("ETag"))
	if etag == "" {
		return "", &Error{Op: "UploadPart", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "missing ETag"}
	}
	return etag, nil
}
func (c *HTTPClient) completeMPU(ctx context.Context, key, id string, parts []completeMPUPart) (string, error) {
	body, err := xml.Marshal(completeMPURequest{Parts: parts})
	if err != nil {
		return "", err
	}
	q := url.Values{"uploadId": {id}}
	u := c.keyURL(key)
	u.RawQuery = sigV4EncodeQuery(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", "application/xml")
	ph := hexSHA256(body)
	req.Header.Set("X-Amz-Content-Sha256", ph)
	c.signer.sign(req, ph, time.Now())
	resp, err := c.client.Do(req)
	if err != nil {
		return "", transportError(ctx, "CompleteMultipartUpload", key, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", decodeS3Error("CompleteMultipartUpload", key, resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var serr s3ErrorResponse
	if xml.Unmarshal(raw, &serr) == nil && serr.Code != "" {
		return "", &Error{Op: "CompleteMultipartUpload", Key: key, StatusCode: resp.StatusCode, Code: serr.Code, Message: serr.Message, Retryable: isRetryableS3Error(resp.StatusCode, serr.Code) || serr.Code == "InternalError" || serr.Code == "ServiceUnavailable"}
	}
	var out completeMPUResult
	if err = xml.Unmarshal(raw, &out); err != nil || out.ETag == "" {
		return "", &Error{Op: "CompleteMultipartUpload", Key: key, StatusCode: resp.StatusCode, Code: "InvalidResponse", Message: "invalid complete response"}
	}
	return unquoteETag(out.ETag), nil
}
func (c *HTTPClient) abortMPU(ctx context.Context, key, id string) error {
	q := url.Values{"uploadId": {id}}
	req, err := c.newRequest(ctx, http.MethodDelete, c.keyURL(key), nil, nil, q)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return transportError(ctx, "AbortMultipartUpload", key, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return decodeS3Error("AbortMultipartUpload", key, resp)
}

var _ MultipartBackend = (*HTTPClient)(nil)
