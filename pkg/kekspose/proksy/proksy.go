package proksy

import (
	"bytes"
	"fmt"
	"io"
	"net"

	"github.com/scholzj/go-kafka-protocol/api/apiversions"
	"github.com/scholzj/go-kafka-protocol/api/findcoordinator"
	"github.com/scholzj/go-kafka-protocol/api/metadata"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/utils/ptr"
)

type Proksy struct {
	NodeId       int32
	PortMapping  map[int32]uint32
	Correlations map[int32]protocol.RequestHeader
}

func NewProksy(nodeId int32, portMapping map[int32]uint32) *Proksy {
	return &Proksy{
		NodeId:       nodeId,
		PortMapping:  portMapping,
		Correlations: make(map[int32]protocol.RequestHeader),
	}
}

func (p *Proksy) BrokerToClient(client net.Conn, broker httpstream.Stream, shutdownChannel chan struct{}) {
	defer client.Close()

	for {
		response, err := protocol.ReadResponse(broker, p.Correlations)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("<- Reached EOF for node %d\n", p.NodeId)
			} else {
				fmt.Printf("<- Failed to read bytes for node %d\n", p.NodeId, err)
				break
			}

			return
		}

		fmt.Printf("<- Response: node=%d; size=%d; apiKey=%d; version=%d; correlationID=%d; clientId=%s; bodySize=%d bytes\n", p.NodeId, response.Size, response.ApiKey, response.ApiVersion, response.CorrelationId, *response.ClientId, response.Body.Len())
		delete(p.Correlations, response.CorrelationId)

		if response.ApiKey == 3 {
			metadataResponse := metadata.MetadataResponse{}
			err := metadataResponse.Read(response)
			if err != nil {
				fmt.Printf("<- Failed to decode Metadata response from node %d: %v\n", p.NodeId, err)
				break
			}

			for i, _ := range *metadataResponse.Brokers {
				(*metadataResponse.Brokers)[i].Host = "localhost"
				(*metadataResponse.Brokers)[i].Port = int32(p.PortMapping[(*metadataResponse.Brokers)[i].NodeId])
			}

			metadataResponse.PrettyPrint()

			buf := bytes.NewBuffer(make([]byte, 0))
			err = metadataResponse.Write(buf)
			if err != nil {
				fmt.Printf("<- Failed to re-encode Metadata response from node %d: %v\n", p.NodeId, err)
				break
			}
			response.Body = buf
		} else if response.ApiKey == 10 {
			findCoordinator := findcoordinator.FindCoordinatorResponse{}
			err := findCoordinator.Read(response)
			if err != nil {
				fmt.Println("Failed to decode FindCoordinator response", err)
			}

			if findCoordinator.Host != nil {
				findCoordinator.Host = ptr.To("localhost")
				findCoordinator.Port = int32(p.PortMapping[findCoordinator.NodeId])
			} else if findCoordinator.Coordinators != nil {
				for i, _ := range *findCoordinator.Coordinators {
					(*findCoordinator.Coordinators)[i].Host = ptr.To("localhost")
					(*findCoordinator.Coordinators)[i].Port = int32(p.PortMapping[(*findCoordinator.Coordinators)[i].NodeId])
				}
			}

			findCoordinator.PrettyPrint()

			buf := bytes.NewBuffer(make([]byte, 0))
			err = findCoordinator.Write(buf)
			if err != nil {
				fmt.Println("Failed to reencode FindCoordinator response", err)
			}
			response.Body = buf
		} else if response.ApiKey == 18 {
			apiVersions := apiversions.ApiVersionsResponse{}
			err := apiVersions.Read(response)
			if err != nil {
				fmt.Println("Failed to decode ApiVersions response", err)
			}

			apiVersions.PrettyPrint()

			buf := bytes.NewBuffer(make([]byte, 0))
			err = apiVersions.Write(buf)
			if err != nil {
				fmt.Println("Failed to reencode ApiVersions response", err)
			}
			response.Body = buf
		}

		fmt.Printf("<- Proxying response from remote for node %d\n", p.NodeId)
		err = response.Write(client)
		if err != nil {
			fmt.Printf("<- Failed to echo bytes for node %d: %v\n", p.NodeId, err)
			break
		}
	}

	// inform the port forward that we are done
	close(shutdownChannel)
}

func (p *Proksy) ClientToBroker(client net.Conn, broker httpstream.Stream, shutdownChannel chan struct{}) {
	defer broker.Close()

	for {
		request, err := protocol.ReadRequest(client)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("-> Reached EOF for node %d\n", p.NodeId)
			} else {
				fmt.Printf("-> Failed to read bytes for node %d\n", p.NodeId, err)
				break
			}

			return
		}

		fmt.Printf("-> Received request: node=%d; size=%d; apiKey=%d; version=%d; correlationID=%d; clientId=%s; bodySize=%d bytes\n", p.NodeId, request.Size, request.ApiKey, request.ApiVersion, request.CorrelationId, *request.ClientId, request.Body.Len())
		p.Correlations[request.CorrelationId] = request.RequestHeader

		if request.ApiKey == 18 {
			apiVersions := apiversions.ApiVersionsRequest{}
			err := apiVersions.Read(request)
			if err != nil {
				fmt.Println("Failed to decode ApiVersions request", err)
			}

			apiVersions.PrettyPrint()

			buf := bytes.NewBuffer(make([]byte, 0))
			err = apiVersions.Write(buf)
			if err != nil {
				fmt.Println("Failed to reencode ApiVersions request", err)
			}
			request.Body = buf
		} else if request.ApiKey == 10 {
			findCoordinator := findcoordinator.FindCoordinatorRequest{}
			err := findCoordinator.Read(request)
			if err != nil {
				fmt.Println("Failed to decode FindCoordinator request", err)
			}

			findCoordinator.PrettyPrint()

			buf := bytes.NewBuffer(make([]byte, 0))
			err = findCoordinator.Write(buf)
			if err != nil {
				fmt.Printf("Failed to re-encode request", err)
			}
			request.Body = buf
		} else if request.ApiKey == 3 {
			metadataRequest := metadata.MetadataRequest{}
			err := metadataRequest.Read(request)
			if err != nil {
				fmt.Println("Failed to decode ApiVersions request", err)
			}

			metadataRequest.PrettyPrint()

			buf := bytes.NewBuffer(make([]byte, 0))
			err = metadataRequest.Write(buf)
			if err != nil {
				fmt.Printf("Failed to re-encode request", err)
			}
			request.Body = buf
		}

		fmt.Printf("-> Proxying request from local to remote for node %d\n", p.NodeId)
		err = request.Write(broker)
		if err != nil {
			fmt.Printf("-> Failed to echo bytes for node %d: %v\n", p.NodeId, err)
			break
		}
	}

	// inform the port forward that we are done
	close(shutdownChannel)
}
