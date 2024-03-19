package center

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
)

func init() {
	clientManager = NewClientManager()
	serviceManager = NewServiceManager()
}

type ClientState int

const ClientStateNone ClientState = 0
const ClientStateOk ClientState = 1
const ClientStateCloseed ClientState = 2

const msgBufferSize = 2048

var clientManager *NodeManager
var serviceManager *ServiceManager

// 接收到的消息
type Message struct {
	OriginID    string      `json:"origin_id,omitempty"`
	ID          string      `json:"id,omitempty"`           // 消息ID, 只要客户端自己唯一即可
	Data        interface{} `json:"data,omitempty"`         // 消息数据
	Type        string      `json:"type,omitempty"`         // 消息类型
	NodeName    string      `json:"node_name,omitempty"`    // 发送给的客户端名称
	ServiceName string      `json:"service_name,omitempty"` // 发送给的服务名称
}

func NewMessage() *Message {
	return &Message{}
}

func (m *Message) SetID(id string) *Message {
	m.ID = id
	return m
}

func (m *Message) SetData(data interface{}) *Message {
	m.Data = data
	return m
}

func (m *Message) SetType(t string) *Message {
	m.Type = t
	return m
}

func (m *Message) SetNodeName(nodeName string) *Message {
	m.NodeName = nodeName
	return m
}

func (m *Message) SetServiceName(serviceName string) *Message {
	m.ServiceName = serviceName
	return m
}

func (m Message) Bytes() []byte {
	b, _ := json.Marshal(m)
	return b
}

func (m Message) String() string {
	return string(m.Bytes())
}

// 认证消息
type DataAuth struct {
	Name string `json:"name"`
}

func NewServer(ctx context.Context, network, address string) error {

	// 监听端口
	listener, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					log.Println("server accept error:", err)
					continue
				}

				go HandleNewConn(conn)
			}
		}
	}()
	// 循环处理客户端连接
	return nil
}

// 处理新连接
func HandleNewConn(conn net.Conn) {
	var buf []byte = make([]byte, 512)

	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	data := &DataAuth{}
	msg := Message{Data: data}
	err = json.Unmarshal(buf[:n], &msg)
	if err != nil {
		return
	}

	if msg.Type != "auth" {
		return
	}

	fmt.Println(msg.String())

	if authConn(*data) != nil {
		return
	}

	conn.Write(NewMessage().SetID(msg.ID).SetType("authed").Bytes())

	clientManager.HandleNewConn(conn, data.Name)
}

func authConn(data DataAuth) error {
	if data.Name == "" {
		return errors.New("name is empty")
	}

	return nil
}
