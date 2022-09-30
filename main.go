package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

const broker = "localhost:9092"
const groupId = "test"
const topic = "test-topic"

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family":    "v4",
		"group.id":                 groupId,
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})
	if err != nil {
		log.Fatalf("failed to create consumer")
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		log.Fatalf("failed to create producer")
	}

	t := topic
	for i := 0; i < 5; i++ {
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &t},
			Value:          []byte(string(i)),
		}
		err := p.Produce(message, nil)
		if err != nil {
			log.Fatalf("failed to produce message")
		}
	}

	log.Println("produced messages")

	c.Subscribe(topic, nil)
	for {
		outMessage, err := c.ReadMessage(10 * time.Second)
		if err != nil {
			log.Fatalf("failed to read message: %s", err)
		}

		log.Printf("got message %s with value %s", outMessage, string(outMessage.Value))
	}

	return
}
