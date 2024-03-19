package center

import (
	"errors"
	"sync"
)

// 微服务
type Service struct {
	Name   string `json:"name"`
	Able   bool   `json:"able"`
	Client *Node  `json:"client"`
}

// 微服务管理器
type ServiceManager struct {
	List    map[string]*Service
	Syncing *sync.WaitGroup
}

func NewServiceManager() *ServiceManager {
	return &ServiceManager{
		List:    make(map[string]*Service),
		Syncing: &sync.WaitGroup{},
	}
}

// 服务注册
func (m *ServiceManager) AddService(name string, c *Node) error {

	s := &Service{
		Name:   name,
		Able:   true,
		Client: c,
	}

	m.List[name] = s

	return nil
}

// 向服务发送消息
func (m *ServiceManager) SendMessage(serviceName string, msg Message) error {
	if serviceName == "" {
		return errors.New("serviceName is empty")
	}

	s, ok := m.List[serviceName]
	if !ok {
		return errors.New("service not found")
	}

	// 发送消息
	s.Client.messageWriteChan <- msg

	return nil
}
