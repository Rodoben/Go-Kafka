package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "HSVC"
	go Consumer(topic)
	Producer(topic)
}
func Producer(topic string) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "something",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	fmt.Printf("%+v\n", p)
	deliveryChan := make(chan kafka.Event, 10000)
	for {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte("FOO")},
			deliveryChan,
		)
		if err != nil {
			log.Fatal(err)
		}
		<-deliveryChan
		time.Sleep(time.Second * 3)
	}

}

func Consumer(topic string) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest"})

	if err != nil {
		log.Fatal(err)
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatal(err)
	}
	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("Consumed Message from the queue:%+v\n", string(e.Value))
		case kafka.Error:
			fmt.Printf("%+v\n", e)
		}
	}
}
