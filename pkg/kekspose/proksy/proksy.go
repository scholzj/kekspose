package proksy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/scholzj/go-kafka-protocol/api/apiversions"
	"github.com/scholzj/go-kafka-protocol/api/describecluster"
	"github.com/scholzj/go-kafka-protocol/api/fetch"
	"github.com/scholzj/go-kafka-protocol/api/findcoordinator"
	"github.com/scholzj/go-kafka-protocol/api/metadata"
	"github.com/scholzj/go-kafka-protocol/api/produce"
	"github.com/scholzj/go-kafka-protocol/api/shareacknowledge"
	"github.com/scholzj/go-kafka-protocol/api/sharefetch"
	"github.com/scholzj/go-kafka-protocol/apis"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/utils/ptr"
)

var (
	TraceLevel = slog.Level(-10)
)

type Proksy struct {
	NodeId      int32
	PortMapping map[int32]uint32
}

type correlationStore struct {
	mu           sync.RWMutex
	correlations map[int32]protocol.RequestHeader
}

type messageWriter interface {
	Write(io.Writer) error
}

type prettyPrinter interface {
	PrettyPrint() string
}

func clientID(clientID *string) string {
	if clientID == nil {
		return ""
	}

	return *clientID
}

func newCorrelationStore() *correlationStore {
	return &correlationStore{
		correlations: make(map[int32]protocol.RequestHeader),
	}
}

func (c *correlationStore) snapshot() map[int32]protocol.RequestHeader {
	c.mu.RLock()
	defer c.mu.RUnlock()

	copy := make(map[int32]protocol.RequestHeader, len(c.correlations))
	for correlationID, header := range c.correlations {
		copy[correlationID] = header
	}

	return copy
}

func (c *correlationStore) set(correlationID int32, header protocol.RequestHeader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.correlations[correlationID] = header
}

func (c *correlationStore) delete(correlationID int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.correlations, correlationID)
}

func (c *correlationStore) get(correlationID int32) (protocol.RequestHeader, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	header, found := c.correlations[correlationID]
	return header, found
}

func (p *Proksy) rewriteHosts(count int, nodeID func(int) int32, rewrite func(int, *string, int32)) error {
	for i := range count {
		id := nodeID(i)
		if id < 0 {
			continue
		}

		mappedPort, found := p.PortMapping[id]
		if !found {
			return fmt.Errorf("no port mapping found for node ID %d", id)
		}

		rewrite(i, ptr.To("localhost"), int32(mappedPort))
	}

	return nil
}

func encodeBody(message messageWriter) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	err := message.Write(buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func traceMessage(message prettyPrinter) {
	slog.Log(context.Background(), TraceLevel, message.PrettyPrint())
}

func readResponse(r io.Reader, correlations *correlationStore) (protocol.Response, error) {
	response := protocol.Response{}

	size, err := protocol.ReadInt32(r)
	if err != nil {
		return response, err
	}
	response.Size = size

	responseReader := io.LimitReader(r, int64(response.Size))

	response.CorrelationId, err = protocol.ReadInt32(responseReader)
	if err != nil {
		return response, err
	}

	requestHeader, ok := correlations.get(response.CorrelationId)
	if !ok {
		return response, fmt.Errorf("no correlation found for correlationId %d", response.CorrelationId)
	}

	response.ApiKey = requestHeader.ApiKey
	response.ApiVersion = requestHeader.ApiVersion
	response.ClientId = requestHeader.ClientId

	if apis.ResponseHeaderVersion(response.ApiKey, response.ApiVersion) >= 1 {
		if _, err := protocol.ReadRawTaggedFields(responseReader); err != nil {
			return response, err
		}
	}

	response.Body, err = readBody(responseReader)
	if err != nil {
		return response, err
	}

	return response, nil
}

func readBody(r io.Reader) (*bytes.Buffer, error) {
	body := bytes.NewBuffer(make([]byte, 0))
	_, err := io.Copy(body, r)
	return body, err
}

func (p *Proksy) rewriteResponse(response *protocol.Response) error {
	switch response.ApiKey {
	case 0:
		return p.rewriteProduceResponse(response)
	case 1:
		return p.rewriteFetchResponse(response)
	case 3:
		return p.rewriteMetadataResponse(response)
	case 10:
		return p.rewriteFindCoordinatorResponse(response)
	case 18:
		return p.rewriteAPIVersionsResponse(response)
	case 60:
		return p.rewriteDescribeClusterResponse(response)
	case 78:
		return p.rewriteShareFetchResponse(response)
	case 79:
		return p.rewriteShareAcknowledgeResponse(response)
	default:
		return nil
	}
}

func (p *Proksy) rewriteRequest(request *protocol.Request) error {
	switch request.ApiKey {
	case 18:
		return p.rewriteAPIVersionsRequest(request)
	case 10:
		return p.rewriteFindCoordinatorRequest(request)
	case 3:
		return p.rewriteMetadataRequest(request)
	default:
		return nil
	}
}

func (p *Proksy) rewriteProduceResponse(response *protocol.Response) error {
	produceResponse := produce.ProduceResponse{}
	if err := produceResponse.Read(*response); err != nil {
		return fmt.Errorf("decode Produce response: %w", err)
	}

	if produceResponse.NodeEndpoints != nil {
		if err := p.rewriteHosts(len(*produceResponse.NodeEndpoints), func(i int) int32 {
			return (*produceResponse.NodeEndpoints)[i].NodeId
		}, func(i int, host *string, port int32) {
			(*produceResponse.NodeEndpoints)[i].Host = host
			(*produceResponse.NodeEndpoints)[i].Port = port
		}); err != nil {
			return err
		}
	}

	traceMessage(&produceResponse)

	buf, err := encodeBody(&produceResponse)
	if err != nil {
		return fmt.Errorf("re-encode Produce response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteFetchResponse(response *protocol.Response) error {
	fetchResponse := fetch.FetchResponse{}
	if err := fetchResponse.Read(*response); err != nil {
		return fmt.Errorf("decode Fetch response: %w", err)
	}

	if fetchResponse.NodeEndpoints != nil {
		if err := p.rewriteHosts(len(*fetchResponse.NodeEndpoints), func(i int) int32 {
			return (*fetchResponse.NodeEndpoints)[i].NodeId
		}, func(i int, host *string, port int32) {
			(*fetchResponse.NodeEndpoints)[i].Host = host
			(*fetchResponse.NodeEndpoints)[i].Port = port
		}); err != nil {
			return err
		}
	}

	traceMessage(&fetchResponse)

	buf, err := encodeBody(&fetchResponse)
	if err != nil {
		return fmt.Errorf("re-encode Fetch response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteMetadataResponse(response *protocol.Response) error {
	metadataResponse := metadata.MetadataResponse{}
	if err := metadataResponse.Read(*response); err != nil {
		return fmt.Errorf("decode Metadata response: %w", err)
	}

	if err := p.rewriteHosts(len(*metadataResponse.Brokers), func(i int) int32 {
		return (*metadataResponse.Brokers)[i].NodeId
	}, func(i int, host *string, port int32) {
		(*metadataResponse.Brokers)[i].Host = host
		(*metadataResponse.Brokers)[i].Port = port
	}); err != nil {
		return err
	}

	traceMessage(&metadataResponse)

	buf, err := encodeBody(&metadataResponse)
	if err != nil {
		return fmt.Errorf("re-encode Metadata response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteFindCoordinatorResponse(response *protocol.Response) error {
	findCoordinatorResponse := findcoordinator.FindCoordinatorResponse{}
	if err := findCoordinatorResponse.Read(*response); err != nil {
		return fmt.Errorf("decode FindCoordinator response: %w", err)
	}

	if findCoordinatorResponse.Host != nil {
		if findCoordinatorResponse.NodeId < 0 {
			traceMessage(&findCoordinatorResponse)

			buf, err := encodeBody(&findCoordinatorResponse)
			if err != nil {
				return fmt.Errorf("re-encode FindCoordinator response: %w", err)
			}
			response.Body = buf

			return nil
		}

		mappedPort, found := p.PortMapping[findCoordinatorResponse.NodeId]
		if !found {
			return fmt.Errorf("no port mapping found for node ID %d", findCoordinatorResponse.NodeId)
		}

		findCoordinatorResponse.Host = ptr.To("localhost")
		findCoordinatorResponse.Port = int32(mappedPort)
	} else if findCoordinatorResponse.Coordinators != nil {
		if err := p.rewriteHosts(len(*findCoordinatorResponse.Coordinators), func(i int) int32 {
			return (*findCoordinatorResponse.Coordinators)[i].NodeId
		}, func(i int, host *string, port int32) {
			(*findCoordinatorResponse.Coordinators)[i].Host = host
			(*findCoordinatorResponse.Coordinators)[i].Port = port
		}); err != nil {
			return err
		}
	}

	traceMessage(&findCoordinatorResponse)

	buf, err := encodeBody(&findCoordinatorResponse)
	if err != nil {
		return fmt.Errorf("re-encode FindCoordinator response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteAPIVersionsResponse(response *protocol.Response) error {
	apiVersionsResponse := apiversions.ApiVersionsResponse{}
	if err := apiVersionsResponse.Read(*response); err != nil {
		return fmt.Errorf("decode ApiVersions response: %w", err)
	}

	traceMessage(&apiVersionsResponse)

	buf, err := encodeBody(&apiVersionsResponse)
	if err != nil {
		return fmt.Errorf("re-encode ApiVersions response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteDescribeClusterResponse(response *protocol.Response) error {
	describeClusterResponse := describecluster.DescribeClusterResponse{}
	if err := describeClusterResponse.Read(*response); err != nil {
		return fmt.Errorf("decode DescribeCluster response: %w", err)
	}

	if describeClusterResponse.EndpointType == 1 && describeClusterResponse.Brokers != nil {
		if err := p.rewriteHosts(len(*describeClusterResponse.Brokers), func(i int) int32 {
			return (*describeClusterResponse.Brokers)[i].BrokerId
		}, func(i int, host *string, port int32) {
			(*describeClusterResponse.Brokers)[i].Host = host
			(*describeClusterResponse.Brokers)[i].Port = port
		}); err != nil {
			return err
		}
	}

	traceMessage(&describeClusterResponse)

	buf, err := encodeBody(&describeClusterResponse)
	if err != nil {
		return fmt.Errorf("re-encode DescribeCluster response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteShareFetchResponse(response *protocol.Response) error {
	shareFetchResponse := sharefetch.ShareFetchResponse{}
	if err := shareFetchResponse.Read(*response); err != nil {
		return fmt.Errorf("decode ShareFetch response: %w", err)
	}

	if shareFetchResponse.NodeEndpoints != nil {
		if err := p.rewriteHosts(len(*shareFetchResponse.NodeEndpoints), func(i int) int32 {
			return (*shareFetchResponse.NodeEndpoints)[i].NodeId
		}, func(i int, host *string, port int32) {
			(*shareFetchResponse.NodeEndpoints)[i].Host = host
			(*shareFetchResponse.NodeEndpoints)[i].Port = port
		}); err != nil {
			return err
		}
	}

	traceMessage(&shareFetchResponse)

	buf, err := encodeBody(&shareFetchResponse)
	if err != nil {
		return fmt.Errorf("re-encode ShareFetch response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteShareAcknowledgeResponse(response *protocol.Response) error {
	shareAcknowledgeResponse := shareacknowledge.ShareAcknowledgeResponse{}
	if err := shareAcknowledgeResponse.Read(*response); err != nil {
		return fmt.Errorf("decode ShareAcknowledge response: %w", err)
	}

	if shareAcknowledgeResponse.NodeEndpoints != nil {
		if err := p.rewriteHosts(len(*shareAcknowledgeResponse.NodeEndpoints), func(i int) int32 {
			return (*shareAcknowledgeResponse.NodeEndpoints)[i].NodeId
		}, func(i int, host *string, port int32) {
			(*shareAcknowledgeResponse.NodeEndpoints)[i].Host = host
			(*shareAcknowledgeResponse.NodeEndpoints)[i].Port = port
		}); err != nil {
			return err
		}
	}

	traceMessage(&shareAcknowledgeResponse)

	buf, err := encodeBody(&shareAcknowledgeResponse)
	if err != nil {
		return fmt.Errorf("re-encode ShareAcknowledge response: %w", err)
	}
	response.Body = buf

	return nil
}

func (p *Proksy) rewriteAPIVersionsRequest(request *protocol.Request) error {
	apiVersionsRequest := apiversions.ApiVersionsRequest{}
	if err := apiVersionsRequest.Read(*request); err != nil {
		return fmt.Errorf("decode ApiVersions request: %w", err)
	}

	traceMessage(&apiVersionsRequest)

	buf, err := encodeBody(&apiVersionsRequest)
	if err != nil {
		return fmt.Errorf("re-encode ApiVersions request: %w", err)
	}
	request.Body = buf

	return nil
}

func (p *Proksy) rewriteFindCoordinatorRequest(request *protocol.Request) error {
	findCoordinatorRequest := findcoordinator.FindCoordinatorRequest{}
	if err := findCoordinatorRequest.Read(*request); err != nil {
		return fmt.Errorf("decode FindCoordinator request: %w", err)
	}

	traceMessage(&findCoordinatorRequest)

	buf, err := encodeBody(&findCoordinatorRequest)
	if err != nil {
		return fmt.Errorf("re-encode FindCoordinator request: %w", err)
	}
	request.Body = buf

	return nil
}

func (p *Proksy) rewriteMetadataRequest(request *protocol.Request) error {
	metadataRequest := metadata.MetadataRequest{}
	if err := metadataRequest.Read(*request); err != nil {
		return fmt.Errorf("decode Metadata request: %w", err)
	}

	traceMessage(&metadataRequest)

	buf, err := encodeBody(&metadataRequest)
	if err != nil {
		return fmt.Errorf("re-encode Metadata request: %w", err)
	}
	request.Body = buf

	return nil
}

func NewProksy(nodeId int32, portMapping map[int32]uint32) *Proksy {
	return &Proksy{
		NodeId:      nodeId,
		PortMapping: portMapping,
	}
}

func (p *Proksy) Proxy(client net.Conn, broker httpstream.Stream, brokerToClientShutdown chan struct{}, clientToBrokerShutdown chan struct{}) {
	correlations := newCorrelationStore()

	go p.BrokerToClient(client, broker, brokerToClientShutdown, correlations)
	go p.ClientToBroker(client, broker, clientToBrokerShutdown, correlations)
}

func (p *Proksy) BrokerToClient(client net.Conn, broker httpstream.Stream, shutdownChannel chan struct{}, correlations *correlationStore) {
	defer client.Close()
	defer close(shutdownChannel)

	for {
		response, err := readResponse(broker, correlations)
		if err != nil {
			if err == io.EOF {
				slog.Debug("<- Reached EOF", "node", p.NodeId)
			} else {
				slog.Error("<- Failed to read bytes", "node", p.NodeId, "error", err)
				break
			}

			return
		}

		slog.Debug("<- Received response", "node", p.NodeId, "size", response.Size, "apiKey", response.ApiKey, "apiVersion", response.ApiVersion, "correlationId", response.CorrelationId, "clientId", clientID(response.ClientId), "bodySize", response.Body.Len())
		correlations.delete(response.CorrelationId)

		if err := p.rewriteResponse(&response); err != nil {
			slog.Error("<- Failed to rewrite response", "node", p.NodeId, "apiKey", response.ApiKey, "error", err)
			break
		}

		slog.Debug("<- Proxying response from remote", "node", p.NodeId)
		err = response.Write(client)
		if err != nil {
			slog.Debug("<- Failed to echo bytes", "node", p.NodeId, "error", err)
			break
		}
	}

}

func (p *Proksy) ClientToBroker(client net.Conn, broker httpstream.Stream, shutdownChannel chan struct{}, correlations *correlationStore) {
	defer broker.Close()
	defer close(shutdownChannel)

	for {
		request, err := protocol.ReadRequest(client)
		if err != nil {
			if err == io.EOF {
				slog.Debug("-> Reached EOF", "node", p.NodeId)
			} else {
				slog.Error("-> Failed to read bytes", "node", p.NodeId, "error", err)
				break
			}

			return
		}

		slog.Debug("-> Received request", "node", p.NodeId, "size", request.Size, "apiKey", request.ApiKey, "apiVersion", request.ApiVersion, "correlationId", request.CorrelationId, "clientId", clientID(request.ClientId), "bodySize", request.Body.Len())
		correlations.set(request.CorrelationId, request.RequestHeader)

		if err := p.rewriteRequest(&request); err != nil {
			slog.Error("-> Failed to rewrite request", "node", p.NodeId, "apiKey", request.ApiKey, "error", err)
			break
		}

		slog.Debug("-> Proxying request from local to remote", "node", p.NodeId)
		err = request.Write(broker)
		if err != nil {
			slog.Error("-> Failed to echo bytes", "node", p.NodeId, "error", err)
			break
		}
	}

}
