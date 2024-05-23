package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"

	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	ID   string     `json:"id"`
	Name string     `json:"name"`
	Text testStruct `json:"text"`
}

type testStruct struct {
	ID        int      `json:"id"`
	Name      string   `json:"name"`
	IsActive  bool     `json:"is_active"`
	Age       int      `json:"age"`
	Height    float64  `json:"height"`
	Languages []string `json:"languages"`
	Address   Address  `json:"address"`
	Metadata  Metadata `json:"metadata"`
}

type Address struct {
	City    string `json:"city"`
	Zipcode string `json:"zipcode"`
	Country string `json:"country"`
}

type Metadata struct {
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Details   Details   `json:"details"`
}

type Details struct {
	Key1 struct {
		Subkey1 string `json:"subkey1"`
		Subkey2 string `json:"subkey2"`
	} `json:"key1"`
	Key2 struct {
		Subkey3 string `json:"subkey3"`
		Subkey4 string `json:"subkey4"`
	} `json:"key2"`
}

var topic string = "Comment"

func main() {

	app := fiber.New()
	api := app.Group("/api/v1") // /api

	api.Post("/comments", createComment)

	app.Listen(":3000")

}

func createComment(c *fiber.Ctx) error {

	fmt.Println("c", c)

	//fmt.Println("c", string(c.Body()))
	cmt := new(Comment)
	fmt.Println(cmt)
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
	}

	jsonComment, err := json.Marshal(cmt)
	if err != nil {
		c.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
	}
	PushCommentTOQueue(topic, jsonComment)

	c.Status(http.StatusOK).JSON(&fiber.Map{
		"Success": true,
		"Message": cmt.ID,
	})

	return nil
}

func PushCommentTOQueue(topic string, message []byte) error {

	baseUrl := []string{"localhost:9092"}

	fmt.Println("connecting")
	producer, err := ConnectProducer(baseUrl)
	if err != nil {
		return err
	}

	fmt.Println("connected")
	defer producer.Close()

	msg := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	patition, offset, err := producer.SendMessage(&msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, patition, offset)

	return nil
}

func ConnectProducer(brokerUrl []string) (sarama.SyncProducer, error) {
	fmt.Println("1")
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	fmt.Println("2", config)
	conn, err := sarama.NewSyncProducer(brokerUrl, config)
	if err != nil {
		return nil, err
	}
	fmt.Println("3", conn)

	return conn, nil
}
