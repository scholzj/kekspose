package proksy

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/scholzj/go-kafka-protocol/api/findcoordinator"
	"github.com/scholzj/go-kafka-protocol/api/metadata"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/utils/ptr"
)

func TestRewriteMetadataResponse(t *testing.T) {
	p := NewProksy(0, map[int32]uint32{1: 50001, 2: 50002})
	b := []metadata.MetadataResponseBroker{
		{NodeId: 1, Host: ptr.To("broker-1"), Port: 9092},
		{NodeId: 2, Host: ptr.To("broker-2"), Port: 9093},
	}
	topics := []metadata.MetadataResponseTopic{}

	response := encodeResponse(t, 3, 0, &metadata.MetadataResponse{Brokers: &b, Topics: &topics})

	require.NoError(t, p.rewriteResponse(&response))

	decoded := metadata.MetadataResponse{}
	require.NoError(t, decoded.Read(response))
	require.NotNil(t, decoded.Brokers)
	assert.Len(t, *decoded.Brokers, 2)
	assert.Equal(t, "localhost", *(*decoded.Brokers)[0].Host)
	assert.Equal(t, int32(50001), (*decoded.Brokers)[0].Port)
	assert.Equal(t, "localhost", *(*decoded.Brokers)[1].Host)
	assert.Equal(t, int32(50002), (*decoded.Brokers)[1].Port)
}

func TestRewriteFindCoordinatorResponse(t *testing.T) {
	p := NewProksy(0, map[int32]uint32{7: 50007})
	response := encodeResponse(t, 10, 0, &findcoordinator.FindCoordinatorResponse{
		NodeId: 7,
		Host:   ptr.To("broker-7"),
		Port:   9092,
	})

	require.NoError(t, p.rewriteResponse(&response))

	decoded := findcoordinator.FindCoordinatorResponse{}
	require.NoError(t, decoded.Read(response))
	require.NotNil(t, decoded.Host)
	assert.Equal(t, "localhost", *decoded.Host)
	assert.Equal(t, int32(50007), decoded.Port)
}

func TestRewriteFindCoordinatorResponseKeepsSentinelNodeID(t *testing.T) {
	p := NewProksy(0, map[int32]uint32{})
	response := encodeResponse(t, 10, 0, &findcoordinator.FindCoordinatorResponse{
		NodeId: -1,
		Host:   ptr.To("unavailable"),
		Port:   9092,
	})

	require.NoError(t, p.rewriteResponse(&response))

	decoded := findcoordinator.FindCoordinatorResponse{}
	require.NoError(t, decoded.Read(response))
	require.NotNil(t, decoded.Host)
	assert.Equal(t, int32(-1), decoded.NodeId)
	assert.Equal(t, "unavailable", *decoded.Host)
	assert.Equal(t, int32(9092), decoded.Port)
}

func TestRewriteResponseUnsupportedAPIKeyIsPassthrough(t *testing.T) {
	p := NewProksy(0, map[int32]uint32{1: 50001})
	response := protocol.Response{
		ResponseHeader: protocol.ResponseHeader{ApiKey: 999, ApiVersion: 0},
		Body:           encodeRawBuffer(t, []byte{1, 2, 3, 4}),
	}
	originalBody := append([]byte(nil), response.Body.Bytes()...)

	require.NoError(t, p.rewriteResponse(&response))
	assert.Equal(t, originalBody, response.Body.Bytes())
}

func TestRewriteMetadataResponseFailsWhenPortMappingMissing(t *testing.T) {
	p := NewProksy(0, map[int32]uint32{})
	b := []metadata.MetadataResponseBroker{{NodeId: 1, Host: ptr.To("broker-1"), Port: 9092}}
	topics := []metadata.MetadataResponseTopic{}
	response := encodeResponse(t, 3, 0, &metadata.MetadataResponse{Brokers: &b, Topics: &topics})

	err := p.rewriteResponse(&response)
	require.EqualError(t, err, "no port mapping found for node ID 1")
}

func TestBrokerToClientClosesShutdownChannelOnEOF(t *testing.T) {
	p := NewProksy(0, map[int32]uint32{})
	client, peer := net.Pipe()
	defer peer.Close()

	shutdown := make(chan struct{})
	go p.BrokerToClient(client, eofStream{}, shutdown, newCorrelationStore())

	select {
	case <-shutdown:
	case <-time.After(2 * time.Second):
		t.Fatal("expected shutdown channel to close on EOF")
	}
}

func TestClientToBrokerClosesShutdownChannelOnEOF(t *testing.T) {
	p := NewProksy(0, map[int32]uint32{})

	shutdown := make(chan struct{})
	go p.ClientToBroker(eofConn{}, eofStream{}, shutdown, newCorrelationStore())

	select {
	case <-shutdown:
	case <-time.After(2 * time.Second):
		t.Fatal("expected shutdown channel to close on EOF")
	}
}

func TestReadResponseUsesCorrelationsAddedAfterReadStarts(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()

	correlations := newCorrelationStore()
	responseCh := make(chan protocol.Response, 1)
	errCh := make(chan error, 1)

	go func() {
		response, err := readResponse(reader, correlations)
		if err != nil {
			errCh <- err
			return
		}

		responseCh <- response
	}()

	correlations.set(42, protocol.RequestHeader{ApiKey: 3, ApiVersion: 0})

	body := encodeRawBuffer(t, []byte{1, 2, 3})
	require.NoError(t, (&protocol.Response{
		ResponseHeader: protocol.ResponseHeader{ApiKey: 3, ApiVersion: 0, CorrelationId: 42},
		Body:           body,
	}).Write(writer))

	select {
	case err := <-errCh:
		t.Fatalf("expected response read to succeed, got error: %v", err)
	case response := <-responseCh:
		assert.Equal(t, int32(42), response.CorrelationId)
		assert.Equal(t, int16(3), response.ApiKey)
		assert.Equal(t, int16(0), response.ApiVersion)
		assert.Equal(t, []byte{1, 2, 3}, response.Body.Bytes())
	case <-time.After(2 * time.Second):
		t.Fatal("expected response reader to finish")
	}
}

func encodeResponse(t *testing.T, apiKey int16, apiVersion int16, message messageWriter) protocol.Response {
	t.Helper()

	body, err := encodeBody(message)
	require.NoError(t, err)

	return protocol.Response{
		ResponseHeader: protocol.ResponseHeader{
			ApiKey:     apiKey,
			ApiVersion: apiVersion,
		},
		Body: body,
	}
}

func encodeRawBuffer(t *testing.T, data []byte) *bytes.Buffer {
	t.Helper()

	buf := bytes.NewBuffer(make([]byte, 0, len(data)))
	_, err := buf.Write(data)
	require.NoError(t, err)

	return buf
}

type eofStream struct{}

type eofConn struct{}

func (eofStream) Read([]byte) (int, error)    { return 0, io.EOF }
func (eofStream) Write(p []byte) (int, error) { return len(p), nil }
func (eofStream) Close() error                { return nil }
func (eofStream) Reset() error                { return nil }
func (eofStream) Headers() http.Header        { return http.Header{} }
func (eofStream) Identifier() uint32          { return 0 }

func (eofConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (eofConn) Write(p []byte) (int, error)      { return len(p), nil }
func (eofConn) Close() error                     { return nil }
func (eofConn) LocalAddr() net.Addr              { return nil }
func (eofConn) RemoteAddr() net.Addr             { return nil }
func (eofConn) SetDeadline(time.Time) error      { return nil }
func (eofConn) SetReadDeadline(time.Time) error  { return nil }
func (eofConn) SetWriteDeadline(time.Time) error { return nil }

var _ httpstream.Stream = eofStream{}
var _ net.Conn = eofConn{}
