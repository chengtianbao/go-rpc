package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	uuid "github.com/satori/go.uuid"
)

const msgBufferSize = 2048

type Node struct {
	network          string
	address          string
	name             string
	conn             net.Conn
	closeFlag        int32
	messageHandlers  map[string]MessageHandler
	serviceHandlers  map[string]ServiceHandler
	messageInChans   map[string]chan<- MessageIn
	single           chan struct{}
	closeReceiveChan chan struct{}
}

func (n *Node) getSingle() {
	n.single <- struct{}{}
}

func (n *Node) releaseSingle() {
	<-n.single
}

func (n *Node) IsClosed() bool {
	return atomic.LoadInt32(&n.closeFlag) == 1
}

func (n *Node) close() {
	log.Printf("close client node %s\f", n.name)
	if n.IsClosed() {
		return
	}
	atomic.StoreInt32(&n.closeFlag, 1)
	n.closeReceiveChan <- struct{}{}
	n.conn.Close()

	// 重连
	time.AfterFunc(2*time.Second, func() {
		n.reConn()
	})

}

func (n *Node) SendMessage(msg MessageOut) MessageIn {

	// 不能给自己发消息
	if msg.NodeName == n.name {
		return *NewInMessage().SetError(errors.New("can't send message to self"))
	}

	// 检查状态
	if n.IsClosed() {
		return *NewInMessage().SetError(errors.New("node is closed"))
	}

	msgBuf := msg.Bytes()
	if len(msgBuf) > msgBufferSize {
		return *NewInMessage().SetError(errors.New("message too long"))
	}

	if _, err := n.conn.Write(msg.Bytes()); err != nil {
		n.close()
		return *NewInMessage().SetError(err)
	}

	// 不需要等待
	if !msg.Wait {
		return *NewInMessage().SetID(msg.ID).SetType("nowait")
	}

	ch := make(chan MessageIn, 1)

	var chS chan<- MessageIn = ch
	var chR <-chan MessageIn = ch

	n.messageInChans[msg.ID] = chS
	log.Printf("node n.messageInChans, node=%s, chans=%v\n", n.name, n.messageInChans)

	// 3秒过期
	time.AfterFunc(3*time.Second, func() {
		n.HandleMessage(*NewInMessage().SetID(msg.ID).SetType("timeout").SetError(errors.New("timeout")))
	})

	return <-chR
}

// 发出的消息
type MessageOut struct {
	ID          string      `json:"id,omitempty"`
	Data        interface{} `json:"data,omitempty"`
	Type        string      `json:"type,omitempty"`
	NodeName    string      `json:"node_name,omitempty"`
	ServiceName string      `json:"service_name,omitempty"`
	Wait        bool        `json:"wait,omitempty"`
}

func NewOutMessage() *MessageOut {
	return &MessageOut{}
}

func (m *MessageOut) SetID(id string) *MessageOut {
	m.ID = id
	return m
}

func (m *MessageOut) SetData(data any) *MessageOut {
	m.Data = data
	return m
}

func (m *MessageOut) SetType(t string) *MessageOut {
	m.Type = t
	return m
}

func (m *MessageOut) SetNodeName(c string) *MessageOut {
	m.NodeName = c
	return m
}

func (m *MessageOut) SetServiceName(s string) *MessageOut {
	m.ServiceName = s
	return m
}

func (m *MessageOut) SetWait(w bool) *MessageOut {
	m.Wait = w
	if m.ID == "" {
		m.ID = uuid.NewV4().String()
	}
	return m
}

func (m MessageOut) Bytes() []byte {
	b, _ := json.Marshal(m)
	return b
}

func (m MessageOut) String() string {
	return string(m.Bytes())
}

// 接收到的消息
type MessageIn struct {
	ID          string `json:"id,omitempty"` // 消息ID, 只要客户端自己唯一即可
	Data        []byte `json:"-"`
	Err         error  `json:"err,omitempty"`
	Type        string `json:"type,omitempty"`
	NodeName    string `json:"node_name,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
}

func NewInMessage() *MessageIn {
	return &MessageIn{}
}

func (m *MessageIn) SetID(id string) *MessageIn {
	m.ID = id
	return m
}

func (m *MessageIn) SetData(data []byte) *MessageIn {
	m.Data = data
	return m
}

func (m *MessageIn) SetType(t string) *MessageIn {
	m.Type = t
	return m
}

func (m *MessageIn) SetNodeName(c string) *MessageIn {
	m.NodeName = c
	return m
}

func (m *MessageIn) SetServiceName(s string) *MessageIn {
	m.ServiceName = s
	return m
}

func (m *MessageIn) SetError(err error) *MessageIn {
	m.Err = err
	return m
}

func (m MessageIn) Bytes() []byte {
	b, _ := json.Marshal(m)
	return b
}

func (m MessageIn) String() string {
	return string(m.Bytes())
}

type MessageHandler func(n *Node, msg MessageIn) error

type ServiceHandler func(n *Node, msg MessageIn) (interface{}, error)

// 注册消息处理函数
func (n *Node) RegistMessageHandler(msgType string, handler MessageHandler) {
	n.messageHandlers[msgType] = handler
}

// 注册服务处理函数
func (n *Node) RegisterService(serviceName string, handler ServiceHandler) error {
	if _, ok := n.serviceHandlers[serviceName]; ok {
		return fmt.Errorf("service %s is exist", serviceName)
	}

	msgIn := n.SendMessage(*NewOutMessage().SetType("service_registry").SetWait(true).SetServiceName(serviceName))
	if msgIn.Err != nil {
		return msgIn.Err
	}

	n.serviceHandlers[serviceName] = handler

	return nil
}

// 请求服务
func (n *Node) RequestService(serviceName string, data interface{}) MessageIn {

	// 不能使用本地服务
	if _, ok := n.serviceHandlers[serviceName]; ok {
		return *NewInMessage().SetError(errors.New("can't request local service"))
	}

	return n.SendMessage(
		*NewOutMessage().
			SetID(uuid.NewV4().String()).
			SetType("service_request").
			SetWait(true).
			SetServiceName(serviceName).
			SetData(data),
	)
}

type DataAuth struct {
	Name string `json:"name"`
}

func NewAuthMessage(name string) *MessageOut {
	return NewOutMessage().SetType("auth").SetData(DataAuth{Name: name})
}

func (nd *Node) init() error {
	atomic.StoreInt32(&nd.closeFlag, 0)
	go nd.readLoop()

	return nil
}

func (nd *Node) reConn() error {
	if !nd.IsClosed() {
		return errors.New("node is not closed")
	}

	for {
		conn, err := NewAuthedConn(nd.name, nd.network, nd.address)
		log.Printf("NewAuthedConn err:%s\n", err)
		if err != nil {
			// 失败后2秒重试
			time.Sleep(2 * time.Second)
			continue
		}

		atomic.StoreInt32(&nd.closeFlag, 1)
		nd.conn = conn
		go nd.readLoop()
		break
	}

	return nil
}

func NewAuthedConn(name, network, address string) (net.Conn, error) {
	log.Println("NewAuthedConn")
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	// 5秒有效
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()

	// 认证成功消息
	authOkChan := make(chan bool)
	defer close(authOkChan)

	// 发送认证
	_, err = conn.Write(NewAuthMessage(name).Bytes())
	if err != nil {
		conn.Close()
		return nil, errors.New("auth error")
	}

	// 接收认证消息
	var buf []byte = make([]byte, 512)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, errors.New("auth error")
	}

	// 解析消息
	var msg MessageIn
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		conn.Close()
		return nil, errors.New("auth error")
	}

	if msg.Type != "authed" {
		conn.Close()
		return nil, errors.New("auth error")
	}

	return conn, nil
}

func NewNode(name, network, address string) (*Node, error) {
	conn, err := NewAuthedConn(name, network, address)
	log.Printf("NewAuthedConn err:%s\n", err)
	if err != nil {
		return nil, err
	}

	// 发送认证消息
	nd := &Node{
		network:          network,
		address:          address,
		name:             name,
		conn:             conn,
		messageInChans:   make(map[string]chan<- MessageIn),
		messageHandlers:  make(map[string]MessageHandler),
		serviceHandlers:  make(map[string]ServiceHandler),
		single:           make(chan struct{}, 1),
		closeReceiveChan: make(chan struct{}),
	}

	// 发送认证消息
	nd.init()

	return nd, nil
}

func (c *Node) readLoop() {

	var buf []byte = make([]byte, msgBufferSize)

	for {
		select {
		case <-c.closeReceiveChan:
			return
		default:
		}

		n, err := c.conn.Read(buf)
		if err != nil {
			c.close()
		} else {

			var msg MessageIn

			if err := json.Unmarshal(buf[:n], &msg); err != nil {
				log.Println("client node readLoop Unmarshal error:", err)
			} else {
				msg.SetData(buf[:n])
				c.HandleMessage(msg)
			}
		}
	}
}

// 处理消息
func (n *Node) HandleMessage(msg MessageIn) error {
	n.getSingle()
	defer n.releaseSingle()

	// wait
	if msg.ID != "" {
		if msgInChan, ok := n.messageInChans[msg.ID]; ok {
			msgInChan <- msg
			delete(n.messageInChans, msg.ID)
			close(msgInChan)
		}
	}

	switch msg.Type {
	case "service_request":
		{
			serviceName := msg.ServiceName
			serviceHandler, ok := n.serviceHandlers[serviceName]
			if !ok {
				return fmt.Errorf("unknow service name: %s", serviceName)
			}

			data, err := serviceHandler(n, msg)
			if err != nil {
				return err
			}

			msgOut := NewOutMessage().SetID(msg.ID).SetData(data).SetType("service_response").SetServiceName(serviceName).SetNodeName(msg.NodeName)
			return n.SendMessage(*msgOut).Err

		}
	case "ping":
		{
			// 服务端收到ping消息 {
			return n.SendMessage(*NewOutMessage().SetType("pong")).Err
		}

	}

	// 找到处理函数
	MessageHandler, ok := n.messageHandlers[msg.Type]
	if !ok {
		return fmt.Errorf("unknow message type: %s", msg.Type)
	}

	return MessageHandler(n, msg)
}
