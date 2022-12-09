package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/google/uuid"

	kafka "github.com/segmentio/kafka-go"
)

func newTopic(conn *kafka.Conn, topic string) {
	conn.CreateTopics(kafka.TopicConfig{
		Topic: topic,
		NumPartitions: 3,
		ReplicationFactor: 3,
	})
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer {
		Addr: kafka.TCP(kafkaURL),
		Topic: topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	// dialleader to kafka docker container
	conn, err := kafka.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
if err != nil {
    panic(err.Error())
}
var controllerConn *kafka.Conn
controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
if err != nil {
    panic(err.Error())
}
defer controllerConn.Close()

newTopic(conn, "test-topic")

// write to kafka
writer := newKafkaWriter("127.0.0.1:9092", "test-topic")
fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprint(uuid.New())),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}
		time.Sleep(1 * time.Second)

defer writer.Close()
	}



}