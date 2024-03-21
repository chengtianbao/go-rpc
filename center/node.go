package center

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func NewClientManager() *NodeManager {
	return &NodeManager{
		List:    make(map[string]*Node),
		Syncing: &sync.WaitGroup{},
	}
}

func (m *NodeManager) HandleNewConn(conn net.Conn, name string) {
	// 判断客户端是否存在
	c, ok := m.List[name]
	if ok {
		log.Println("旧链接 客户端已存在")
		c.ReInit(conn)
	} else {
		log.Println("新链接 客户端不存在")
		c := &Node{
			name:               name,
			createAt:           time.Now(),
			pongAt:             time.Now(),
			conn:               conn,
			closeFlag:          0,
			closeReceiveChan:   make(chan struct{}),
			messageReceiveChan: make(chan Message, 10),
			closeWriteChan:     make(chan struct{}),
			messageWriteChan:   make(chan Message, 10),
			closeHandleChan:    make(chan struct{}),
			closePingChan:      make(chan struct{}),
		}

		m.List[name] = c
		c.Init()
	}

}

func (m *NodeManager) SendMessage(nodeName string, msg Message) error {
	if nodeName == "" {
		return fmt.Errorf("client name is empty")
	}

	c, ok := m.List[nodeName]
	if !ok {
		return fmt.Errorf("client not found : %s", nodeName)
	}

	c.messageWriteChan <- msg

	return nil
}

// Node
type Node struct {
	name               string        // 节点名称
	createAt           time.Time     // 创建时间
	pongAt             time.Time     // 上次心跳时间
	conn               net.Conn      // net.Conn
	closeFlag          int32         // 关闭标识
	closeReceiveChan   chan struct{} // 关闭接收通道
	messageReceiveChan chan Message  // 接收消息通道
	closeWriteChan     chan struct{} // 关闭发送通道
	messageWriteChan   chan Message  // 发送消息通道
	closeHandleChan    chan struct{} // 关闭处理通道
	closePingChan      chan struct{} // 关闭心跳通道
}

func (c *Node) IP() string {
	return c.conn.RemoteAddr().String()
}

// IsClosed indicates whether or not the connection is closed
func (c *Node) IsClosed() bool {
	return atomic.LoadInt32(&c.closeFlag) == 1
}

// 客户端管理器
type NodeManager struct {
	List    map[string]*Node
	Syncing *sync.WaitGroup
}

func (c *Node) close() {
	log.Printf("center close note %s\f", c.name)
	if c.IsClosed() {
		return
	}

	atomic.StoreInt32(&c.closeFlag, 1)
	c.conn.Close()
	c.closeReceiveChan <- struct{}{}
	c.closeWriteChan <- struct{}{}
	c.closeHandleChan <- struct{}{}
	c.closePingChan <- struct{}{}
}

func (c *Node) readLoop() {
	defer log.Println("center readLoop closed")

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
			var msg Message

			log.Println("center readLoop:", string(buf[:n]))

			if err := json.Unmarshal(buf[:n], &msg); err != nil {
				log.Println("center Unmarshal error:", err)
			} else {
				c.messageReceiveChan <- msg
			}
		}
	}
}

func (c *Node) writeLoop() {
	defer log.Println("center writeLoop closed")

	for {
		select {
		case <-c.closeWriteChan:
			return
		case msg := <-c.messageWriteChan:
			log.Printf("center write loop received:%s\n", msg)
			if _, err := c.conn.Write(msg.Bytes()); err != nil {
				c.close()
			}
		}
	}
}

func (c *Node) handleLoop() {
	defer log.Println("center handleLoop closed")
	for {
		select {
		case <-c.closeHandleChan:
			return
		case msg := <-c.messageReceiveChan:
			c.HandleMessage(msg)
		}
	}
}

// 处理消息
func (c *Node) HandleMessage(msg Message) error {

	var err error

	switch msg.Type {
	// pong
	case "pong":
		{
			c.pongAt = time.Now()
			return nil
		}
	// 服务注册
	case "service_registry":
		{

			if err := serviceManager.AddService(msg.ServiceName, c); err != nil {
				return err
			}

			c.messageWriteChan <- *NewMessage().SetID(msg.ID).SetType("service_registed")
		}
	// 请求服务
	case "service_request":
		{
			return serviceManager.SendMessage(msg.ServiceName, *NewMessage().
				SetID(msg.ID).
				SetData(msg.Data).
				SetType("service_request").
				SetNodeName(c.name).
				SetServiceName(msg.ServiceName),
			)
		}
	// 响应服务
	case "service_response":
		{
			return clientManager.SendMessage(msg.NodeName, *NewMessage().
				SetID(msg.ID).
				SetData(msg.Data).
				SetType("service_response").
				SetNodeName(c.name).
				SetServiceName(msg.ServiceName),
			)
		}
	// 向客户端发送消息
	case "to_node":
		{
			return clientManager.SendMessage(msg.NodeName, *NewMessage().
				SetID(msg.ID).
				SetData(msg.Data).
				SetType("from_node").
				SetNodeName(c.name),
			)
		}
	// 服务已注册
	case "service_registed":
		{
			return nil
		}

	default:
		err = fmt.Errorf("unknow message type: %s", msg.Type)
	}

	return err
}

func (c *Node) listenPong() {
	t := time.NewTicker(15 * time.Second)
	defer func() {
		t.Stop()
	}()

	for {
		select {
		case <-c.closePingChan:
			{
				return
			}
		case <-t.C:
			{
				// 如果超过10秒没有收到消息, 则认为客户端已经断开连接
				if time.Since(c.pongAt) > 60*time.Second {
					c.close()
				}

				c.messageWriteChan <- *NewMessage().SetType("ping")
			}
		}
	}
}

// 初始
func (c *Node) Init() {
	go c.listenPong()
	go c.readLoop()
	go c.writeLoop()
	go c.handleLoop()
}

// 重新初始化
func (c *Node) ReInit(conn net.Conn) {
	c.close()
	atomic.StoreInt32(&c.closeFlag, 0)
	c.conn = conn
	c.Init()

}
