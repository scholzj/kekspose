package proksy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"

	"github.com/scholzj/go-kafka-protocol/api/apiversions"
	"github.com/scholzj/go-kafka-protocol/api/describecluster"
	"github.com/scholzj/go-kafka-protocol/api/fetch"
	"github.com/scholzj/go-kafka-protocol/api/findcoordinator"
	"github.com/scholzj/go-kafka-protocol/api/metadata"
	"github.com/scholzj/go-kafka-protocol/api/produce"
	"github.com/scholzj/go-kafka-protocol/api/shareacknowledge"
	"github.com/scholzj/go-kafka-protocol/api/sharefetch"
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

func NewProksy(nodeId int32, portMapping map[int32]uint32) *Proksy {
	return &Proksy{
		NodeId:      nodeId,
		PortMapping: portMapping,
	}
}

func (p *Proksy) Proxy(client net.Conn, broker httpstream.Stream, brokerToClientShutdown chan struct{}, clientToBrokerShutdown chan struct{}) {
	correlations := make(map[int32]protocol.RequestHeader)

	go p.BrokerToClient(client, broker, brokerToClientShutdown, correlations)
	go p.ClientToBroker(client, broker, clientToBrokerShutdown, correlations)
}

func (p *Proksy) BrokerToClient(client net.Conn, broker httpstream.Stream, shutdownChannel chan struct{}, correlations map[int32]protocol.RequestHeader) {
	defer client.Close()

	for {
		response, err := protocol.ReadResponse(broker, correlations)
		if err != nil {
			if err == io.EOF {
				slog.Debug("<- Reached EOF", "node", p.NodeId)
			} else {
				slog.Error("<- Failed to read bytes", "node", p.NodeId, "error", err)
				break
			}

			return
		}

		slog.Debug("<- Received response", "node", p.NodeId, "size", response.Size, "apiKey", response.ApiKey, "apiVersion", response.ApiVersion, "correlationId", response.CorrelationId, "clientId", *response.ClientId, "bodySize", response.Body.Len())
		delete(correlations, response.CorrelationId)

		if response.ApiKey == 0 {
			produceResponse := produce.ProduceResponse{}
			err := produceResponse.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode Produce response", "node", p.NodeId, "error", err)
				break
			}

			if produceResponse.NodeEndpoints != nil {
				for i := range *produceResponse.NodeEndpoints {
					(*produceResponse.NodeEndpoints)[i].Host = ptr.To("localhost")
					(*produceResponse.NodeEndpoints)[i].Port = int32(p.PortMapping[(*produceResponse.NodeEndpoints)[i].NodeId])
				}
			}

			slog.Log(context.Background(), TraceLevel, produceResponse.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = produceResponse.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode Produce response", "node", p.NodeId, "error", err)
				break
			}
			response.Body = buf
		} else if response.ApiKey == 1 {
			fetchResponse := fetch.FetchResponse{}
			err := fetchResponse.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode Fetch response", "node", p.NodeId, "error", err)
				break
			}

			if fetchResponse.NodeEndpoints != nil {
				for i := range *fetchResponse.NodeEndpoints {
					(*fetchResponse.NodeEndpoints)[i].Host = ptr.To("localhost")
					(*fetchResponse.NodeEndpoints)[i].Port = int32(p.PortMapping[(*fetchResponse.NodeEndpoints)[i].NodeId])
				}
			}

			slog.Log(context.Background(), TraceLevel, fetchResponse.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = fetchResponse.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode Fetch response", "node", p.NodeId, "error", err)
				break
			}
			response.Body = buf
		} else if response.ApiKey == 3 {
			metadataResponse := metadata.MetadataResponse{}
			err := metadataResponse.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode Metadata response", "node", p.NodeId, "error", err)
				break
			}

			for i := range *metadataResponse.Brokers {
				(*metadataResponse.Brokers)[i].Host = ptr.To("localhost")
				(*metadataResponse.Brokers)[i].Port = int32(p.PortMapping[(*metadataResponse.Brokers)[i].NodeId])
			}

			slog.Log(context.Background(), TraceLevel, metadataResponse.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = metadataResponse.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode Metadata response", "node", p.NodeId, "error", err)
				break
			}
			response.Body = buf
		} else if response.ApiKey == 10 {
			findCoordinator := findcoordinator.FindCoordinatorResponse{}
			err := findCoordinator.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode FindCoordinator response", "node", p.NodeId, "error", err)
			}

			if findCoordinator.Host != nil {
				findCoordinator.Host = ptr.To("localhost")
				findCoordinator.Port = int32(p.PortMapping[findCoordinator.NodeId])
			} else if findCoordinator.Coordinators != nil {
				for i := range *findCoordinator.Coordinators {
					(*findCoordinator.Coordinators)[i].Host = ptr.To("localhost")
					(*findCoordinator.Coordinators)[i].Port = int32(p.PortMapping[(*findCoordinator.Coordinators)[i].NodeId])
				}
			}

			slog.Log(context.Background(), TraceLevel, findCoordinator.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = findCoordinator.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode FindCoordinator response", "node", p.NodeId, "error", err)
			}
			response.Body = buf
		} else if response.ApiKey == 18 {
			apiVersionsResponse := apiversions.ApiVersionsResponse{}
			err := apiVersionsResponse.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode ApiVersions response", "node", p.NodeId, "error", err)
			}

			slog.Log(context.Background(), TraceLevel, apiVersionsResponse.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = apiVersionsResponse.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode ApiVersions response", "node", p.NodeId, "error", err)
			}
			response.Body = buf
		} else if response.ApiKey == 60 {
			describeClusterResponse := describecluster.DescribeClusterResponse{}
			err := describeClusterResponse.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode DescribeCluster response", "node", p.NodeId, "error", err)
			}

			if describeClusterResponse.EndpointType == 1 && describeClusterResponse.Brokers != nil {
				for i := range *describeClusterResponse.Brokers {
					(*describeClusterResponse.Brokers)[i].Host = ptr.To("localhost")
					(*describeClusterResponse.Brokers)[i].Port = int32(p.PortMapping[(*describeClusterResponse.Brokers)[i].BrokerId])
				}
			}

			slog.Log(context.Background(), TraceLevel, describeClusterResponse.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = describeClusterResponse.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode DescribeCluster response", "node", p.NodeId, "error", err)
			}
			response.Body = buf
		} else if response.ApiKey == 78 {
			shareFetchResponse := sharefetch.ShareFetchResponse{}
			err := shareFetchResponse.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode ShareFetch response", "node", p.NodeId, "error", err)
			}

			if shareFetchResponse.NodeEndpoints != nil {
				for i := range *shareFetchResponse.NodeEndpoints {
					(*shareFetchResponse.NodeEndpoints)[i].Host = ptr.To("localhost")
					(*shareFetchResponse.NodeEndpoints)[i].Port = int32(p.PortMapping[(*shareFetchResponse.NodeEndpoints)[i].NodeId])
				}
			}

			slog.Log(context.Background(), TraceLevel, shareFetchResponse.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = shareFetchResponse.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode ShareFetch response", "node", p.NodeId, "error", err)
			}
			response.Body = buf
		} else if response.ApiKey == 79 {
			shareAcknowledgeResponse := shareacknowledge.ShareAcknowledgeResponse{}
			err := shareAcknowledgeResponse.Read(response)
			if err != nil {
				slog.Error("<- Failed to decode ShareAcknowledge response", "node", p.NodeId, "error", err)
			}

			if shareAcknowledgeResponse.NodeEndpoints != nil {
				for i := range *shareAcknowledgeResponse.NodeEndpoints {
					(*shareAcknowledgeResponse.NodeEndpoints)[i].Host = ptr.To("localhost")
					(*shareAcknowledgeResponse.NodeEndpoints)[i].Port = int32(p.PortMapping[(*shareAcknowledgeResponse.NodeEndpoints)[i].NodeId])
				}
			}

			slog.Log(context.Background(), TraceLevel, shareAcknowledgeResponse.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = shareAcknowledgeResponse.Write(buf)
			if err != nil {
				slog.Error("<- Failed to re-encode ShareAcknowledge response", "node", p.NodeId, "error", err)
			}
			response.Body = buf
		}

		slog.Debug("<- Proxying response from remote", "node", p.NodeId)
		err = response.Write(client)
		if err != nil {
			slog.Debug("<- Failed to echo bytes", "node", p.NodeId, "error", err)
			break
		}
	}

	// inform the port forward that we are done
	close(shutdownChannel)
}

func (p *Proksy) ClientToBroker(client net.Conn, broker httpstream.Stream, shutdownChannel chan struct{}, correlations map[int32]protocol.RequestHeader) {
	defer broker.Close()

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

		slog.Debug("-> Received request", "node", p.NodeId, "size", request.Size, "apiKey", request.ApiKey, "apiVersion", request.ApiVersion, "correlationId", request.CorrelationId, "clientId", *request.ClientId, "bodySize", request.Body.Len())
		correlations[request.CorrelationId] = request.RequestHeader

		if request.ApiKey == 18 {
			apiVersions := apiversions.ApiVersionsRequest{}
			err := apiVersions.Read(request)
			if err != nil {
				slog.Error("Failed to decode ApiVersions request", "error", err)
			}

			slog.Log(context.Background(), TraceLevel, apiVersions.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = apiVersions.Write(buf)
			if err != nil {
				slog.Error("Failed to re-encode ApiVersions request", "error", err)
			}
			request.Body = buf
		} else if request.ApiKey == 10 {
			findCoordinator := findcoordinator.FindCoordinatorRequest{}
			err := findCoordinator.Read(request)
			if err != nil {
				slog.Error("Failed to decode FindCoordinator request", "error", err)
			}

			slog.Log(context.Background(), TraceLevel, findCoordinator.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = findCoordinator.Write(buf)
			if err != nil {
				slog.Error("Failed to re-encode request", "error", err)
			}
			request.Body = buf
		} else if request.ApiKey == 3 {
			metadataRequest := metadata.MetadataRequest{}
			err := metadataRequest.Read(request)
			if err != nil {
				slog.Error("Failed to decode Metadata request", "error", err)
			}

			slog.Log(context.Background(), TraceLevel, metadataRequest.PrettyPrint())

			buf := bytes.NewBuffer(make([]byte, 0))
			err = metadataRequest.Write(buf)
			if err != nil {
				slog.Error("Failed to re-encode request", "error", err)
			}
			request.Body = buf
		}

		slog.Debug("-> Proxying request from local to remote", "node", p.NodeId)
		err = request.Write(broker)
		if err != nil {
			slog.Error("-> Failed to echo bytes", "node", p.NodeId, "error", err)
			break
		}
	}

	// inform the port forward that we are done
	close(shutdownChannel)
}
