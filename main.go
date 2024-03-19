package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/chengtianbao/go-rpc/center"
	"github.com/chengtianbao/go-rpc/node"
)

var ctx = context.Background()

func newNode1() (*node.Node, error) {

	n, err := node.NewNode("node1", "tcp", "127.0.0.1:9100")
	if err != nil {
		return nil, err
	}
	log.Println("newnode1 err", err)

	n.RegistMessageHandler("authed", func(node *node.Node, msg node.MessageIn) error {

		return nil
	})
	n.RegistMessageHandler("ping", func(nd *node.Node, msg node.MessageIn) error {

		fmt.Println("RegistMessageHandler ping1")
		msgIn := nd.SendMessage(node.MessageOut{
			Type: "pong",
		})

		log.Println("ping1 msgIn", msgIn.String())

		return nil
	})

	// 处理接收消息
	n.RegistMessageHandler("from_node", func(n *node.Node, msg node.MessageIn) error {

		time.Sleep(1 * time.Second)

		// 响应
		msgIn := n.SendMessage(node.MessageOut{
			ID:       msg.ID,
			Type:     "to_node",
			Data:     "hello " + msg.NodeName,
			NodeName: msg.NodeName,
		})
		log.Println("msgIn1", msgIn.String())

		return nil
	})

	// 注册服务
	n.RegisterService("service1", func(n *node.Node, msg node.MessageIn) (interface{}, error) {

		type MessageData struct {
			Data []string `json:"data"`
		}

		var data MessageData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			log.Println("data type error", err)
			return "request data invalid", fmt.Errorf("data type error")
		}

		log.Println("xxxxx", data)

		return data.Data, nil
	})

	return n, err
}

func newNode2() (*node.Node, error) {

	log.Println("newNode2")

	n, err := node.NewNode("node2", "tcp", "127.0.0.1:9100")
	if err != nil {
		log.Println("newnode1 err", err)
		return nil, err
	}

	log.Println("newnode1 err", err)

	// 处理接收消息
	n.RegistMessageHandler("from_node", func(n *node.Node, msg node.MessageIn) error {

		time.Sleep(1 * time.Second)

		// 响应
		msgIn := n.SendMessage(node.MessageOut{
			ID:       msg.ID,
			Type:     "to_node",
			Data:     "hello " + msg.NodeName,
			NodeName: msg.NodeName,
		})
		log.Println("msgIn2", msgIn.String())

		return nil
	})

	// 注册服务

	return n, err
}

func main() {

	if err := center.NewServer(ctx, "tcp", "127.0.0.1:9100"); err != nil {
		log.Fatalln(err)
	}

	n1, _ := newNode1()
	log.Println("xxxx")
	n2, _ := newNode2()

	data1 := n1.RequestService("service1", []string{"3", "2", "1"})
	log.Println("service1 response data err", data1.Err.Error())

	data2 := n2.RequestService("service1", []string{"4", "5", "6"})
	log.Println("service1 response data", data2, string(data2.Data))

	forever := make(chan struct{})
	<-forever
}
