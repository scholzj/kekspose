package proxiedforward

import (
	"net"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/util/httpstream"
)

type streamConn struct {
	stream httpstream.Stream
}

func newStreamConn(stream httpstream.Stream) net.Conn {
	return &streamConn{stream: stream}
}

func (s *streamConn) Read(p []byte) (int, error) {
	return s.stream.Read(p)
}

func (s *streamConn) Write(p []byte) (int, error) {
	return s.stream.Write(p)
}

func (s *streamConn) Close() error {
	return s.stream.Close()
}

func (s *streamConn) LocalAddr() net.Addr {
	return streamAddr{s.stream.Headers()}
}

func (s *streamConn) RemoteAddr() net.Addr {
	return streamAddr{s.stream.Headers()}
}

func (s *streamConn) SetDeadline(time.Time) error {
	return nil
}

func (s *streamConn) SetReadDeadline(time.Time) error {
	return nil
}

func (s *streamConn) SetWriteDeadline(time.Time) error {
	return nil
}

type streamAddr struct {
	headers http.Header
}

func (s streamAddr) Network() string {
	return "stream"
}

func (s streamAddr) String() string {
	if port := s.headers.Get("port"); port != "" {
		return port
	}
	return "stream"
}
