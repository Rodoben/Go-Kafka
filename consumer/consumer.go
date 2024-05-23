package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "Comment"

	conn, err := ConnectConsumer([]string{"localhost:9092"})

	if err != nil {
		panic(err)
	}

	fmt.Println("conn", conn, topic)

	pc, err := conn.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	//fmt.Println(pc.Messages())

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneChan := make(chan struct{})

	go func() {

		for {
			select {
			case err := <-pc.Errors():
				fmt.Println("err", err)
			case msg := <-pc.Messages():
				msgCount++
				fmt.Printf("Received message count %d: | Topic(%s) | message (%s) \n", msgCount, topic, string(msg.Value))
				fmt.Println(string(msg.Value))

			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneChan <- struct{}{}
			}
		}

	}()

	<-doneChan
	fmt.Println("Processed", msgCount, "messages")
	if err := conn.Close(); err != nil {
		panic(err)
	}

}

func ConnectConsumer(brokerurl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokerurl, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}
