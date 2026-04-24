package proxiedforward

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/httpstream"
)

func TestNewStreamConnWrapsStream(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	stream := &testStream{Conn: left, headers: http.Header{"Port": []string{"9092"}}}
	conn := newStreamConn(stream)

	assert.Equal(t, "stream", conn.LocalAddr().Network())
	assert.Equal(t, "9092", conn.LocalAddr().String())
	assert.Equal(t, "9092", conn.RemoteAddr().String())
	require.NoError(t, conn.SetDeadline(time.Now()))
	require.NoError(t, conn.SetReadDeadline(time.Now()))
	require.NoError(t, conn.SetWriteDeadline(time.Now()))

	go func() {
		_, _ = right.Write([]byte("ping"))
	}()

	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("ping"), buf)
}

func TestEstablishBrokerConnWithoutTLSReturnsStream(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	stream := &testStream{Conn: left, headers: http.Header{"Port": []string{"9092"}}}
	conn, err := establishBrokerConn(stream, false)
	require.NoError(t, err)
	require.Same(t, stream, conn)

	go func() {
		_, _ = right.Write([]byte("pong"))
	}()

	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("pong"), buf)
}

func TestEstablishBrokerConnWithTLS(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	cert := generateTestCertificate(t)
	serverErr := make(chan error, 1)
	go func() {
		server := tls.Server(right, &tls.Config{Certificates: []tls.Certificate{cert}})
		defer server.Close()

		if err := server.Handshake(); err != nil {
			serverErr <- err
			return
		}

		buf := make([]byte, 4)
		if _, err := io.ReadFull(server, buf); err != nil {
			serverErr <- err
			return
		}
		if string(buf) != "ping" {
			serverErr <- io.ErrUnexpectedEOF
			return
		}

		_, err := server.Write([]byte("pong"))
		serverErr <- err
	}()

	stream := &testStream{Conn: left, headers: http.Header{"Port": []string{"9093"}}}
	conn, err := establishBrokerConn(stream, true)
	require.NoError(t, err)

	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)

	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("pong"), buf)
	require.NoError(t, <-serverErr)
}

type testStream struct {
	net.Conn
	headers http.Header
}

func (t *testStream) Reset() error {
	return nil
}

func (t *testStream) Headers() http.Header {
	return t.headers
}

func (t *testStream) Identifier() uint32 {
	return 0
}

func generateTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	return tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}
}

var _ httpstream.Stream = (*testStream)(nil)
