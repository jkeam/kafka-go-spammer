package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	// env vars
	times, parseTimesErr := getEnvInt("TIMES", 1)
	if parseTimesErr != nil {
		log.Println("Error parsing TIMES")
		log.Fatal(parseTimesErr)
	}
	topicName := getEnvString("TOPIC_NAME", "myTopic")

	// producer
	producer, err := createProducer()
	if err != nil {
		log.Fatal(err)
	}

	// start async result printer
	done := make(chan bool)
	go processResult(done, producer, times)

	// send messages
	messageChan := producer.ProduceChannel()
	for i := 0; i < times; i++ {
		word := strconv.Itoa(i)
		messageChan <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}
	}

	// wait and close
	_ = <-done
	close(done)
	producer.Close()
}

func createProducer() (*kafka.Producer, error) {
	config := &kafka.ConfigMap{}

	// get env vars
	kafkaHost := getEnvString("KAFKA_BOOTSTRAP_HOST", "localhost")
	kafkaPort, parsePortErr := getEnvInt("KAFKA_BOOTSTRAP_PORT", 9092)
	if parsePortErr != nil {
		log.Println("Error parsing kafka port")
		return nil, parsePortErr
	}

	// create producer
	bootstrapServer := hostPortType{Host: kafkaHost, Port: kafkaPort}
	setErr := config.SetKey("bootstrap.servers", bootstrapServer)
	if setErr != nil {
		log.Println("Error setting bootstrap server config")
		return nil, setErr
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Println("Error creating producer")
		return nil, err
	}

	return producer, nil
}

func processResult(done chan bool, producer *kafka.Producer, times int) {
	if times <= 0 {
		done <- true
		return
	}
	for event := range producer.Events() {
		switch message := event.(type) {
		case *kafka.Message:
			if message.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", message.TopicPartition)
			} else {
				fmt.Printf("Delivered message to %v\n", message.TopicPartition)
			}
			times--
			if times == 0 {
				done <- true
				return
			}
		default:
			fmt.Printf("Unknown type.")
			fmt.Print(message)
		}
	}
}

func getEnvString(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	val := getEnvString(key, strconv.Itoa(fallback))
	return strconv.Atoi(val)
}

// HostPortType host and port of the bootstrap
type hostPortType struct {
	Host string
	Port int
}

func (hp hostPortType) String() string {
	return fmt.Sprintf("%s:%d", hp.Host, hp.Port)
}
