package s3backend

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// sigV4Algorithm is the AWS Signature Version 4 algorithm identifier used in
// the Authorization header and the string to sign.
const sigV4Algorithm = "AWS4-HMAC-SHA256"

// sigV4Signer signs HTTP requests with AWS Signature Version 4
// (Authorization-header variant; presigned URLs are not supported).
//
// The signer signs exactly the Host header, the Content-Type header when
// present, and every X-Amz-* header present on the request. Callers are
// expected to set any payload-hash header (S3 requires X-Amz-Content-Sha256)
// before signing.
type sigV4Signer struct {
	accessKey    string
	secretKey    string
	sessionToken string
	region       string
	service      string // "s3" in production; parameterized for test vectors
}

// sign adds the X-Amz-Date, optional X-Amz-Security-Token, and Authorization
// headers to req. payloadHash is the lowercase hex SHA-256 of the request
// payload; signingTime is typically time.Now().
func (s *sigV4Signer) sign(req *http.Request, payloadHash string, signingTime time.Time) {
	t := signingTime.UTC()
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")

	req.Header.Set("X-Amz-Date", amzDate)
	if s.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.sessionToken)
	}

	scope := dateStamp + "/" + s.region + "/" + s.service + "/aws4_request"
	canonicalRequest, signedHeaders := sigV4CanonicalRequest(req, payloadHash)
	stringToSign := sigV4Algorithm + "\n" +
		amzDate + "\n" +
		scope + "\n" +
		hexSHA256([]byte(canonicalRequest))
	signingKey := sigV4DeriveKey(s.secretKey, dateStamp, s.region, s.service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	req.Header.Set("Authorization", sigV4Algorithm+
		" Credential="+s.accessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+
		", Signature="+signature)
}

// sigV4CanonicalRequest builds the SigV4 canonical request for req and
// returns it together with the semicolon-separated signed header list.
func sigV4CanonicalRequest(req *http.Request, payloadHash string) (canonicalRequest, signedHeaders string) {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	// Signed headers: Host, Content-Type when set, and every X-Amz-* header.
	signed := map[string]string{"host": host}
	for name, values := range req.Header {
		lname := strings.ToLower(name)
		if lname == "content-type" || strings.HasPrefix(lname, "x-amz-") {
			signed[lname] = strings.Join(values, ",")
		}
	}
	names := make([]string, 0, len(signed))
	for n := range signed {
		names = append(names, n)
	}
	sort.Strings(names)

	var headers strings.Builder
	for _, n := range names {
		headers.WriteString(n)
		headers.WriteByte(':')
		headers.WriteString(sigV4TrimHeaderValue(signed[n]))
		headers.WriteByte('\n')
	}
	signedHeaders = strings.Join(names, ";")

	uri := req.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	return req.Method + "\n" +
		uri + "\n" +
		sigV4CanonicalQuery(req.URL.RawQuery) + "\n" +
		headers.String() + "\n" +
		signedHeaders + "\n" +
		payloadHash, signedHeaders
}

// sigV4TrimHeaderValue trims leading/trailing whitespace and compresses
// sequential spaces, as required for canonical header values.
func sigV4TrimHeaderValue(v string) string {
	return strings.Join(strings.Fields(v), " ")
}

// sigV4CanonicalQuery parses rawQuery and re-encodes it canonically: strict
// RFC 3986 percent-encoding, parameters sorted by encoded name then value.
func sigV4CanonicalQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		// Malformed query: sign the exact bytes that will be sent.
		return rawQuery
	}
	return sigV4EncodeQuery(values)
}

// sigV4EncodeQuery encodes query values with strict SigV4 percent-encoding
// (notably, space is %20, never '+'), sorted by encoded name then value.
// The result is suitable both as a URL RawQuery and as a canonical query
// string, guaranteeing the signed bytes equal the sent bytes.
func sigV4EncodeQuery(values url.Values) string {
	type pair struct{ k, v string }
	pairs := make([]pair, 0, len(values))
	for k, vs := range values {
		for _, v := range vs {
			pairs = append(pairs, pair{sigV4Escape(k), sigV4Escape(v)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

// sigV4Escape percent-encodes s per RFC 3986: every byte except the
// unreserved characters A-Z a-z 0-9 - _ . ~ is encoded as %XX with uppercase
// hex. Multi-byte UTF-8 characters are encoded byte by byte.
func sigV4Escape(s string) string {
	const hexUpper = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexUpper[c>>4])
			b.WriteByte(hexUpper[c&0x0F])
		}
	}
	return b.String()
}

// sigV4EscapePath escapes a slash-separated key path segment by segment,
// keeping the '/' separators intact.
func sigV4EscapePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = sigV4Escape(s)
	}
	return strings.Join(segs, "/")
}

// hmacSHA256 returns HMAC-SHA256(key, data).
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// hexSHA256 returns the lowercase hex SHA-256 of data.
func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// sigV4DeriveKey derives the SigV4 signing key for the given secret, date
// stamp (yyyymmdd), region, and service via the HMAC chain
// Date <- Region <- Service <- "aws4_request".
func sigV4DeriveKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}
