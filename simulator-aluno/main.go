package main

import (
	"fmt"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	kafka2 "github.com/fernandoguide/imersaofsfc2-simulator/simulator-aluno/application/kafka"
	"github.com/fernandoguide/imersaofsfc2-simulator/simulator-aluno/infra/kafka"
	"github.com/joho/godotenv"
	"log"
)
// kafka-console-producer --bootstrap-server=localhost92 --topic=route.new-direction
// kafka-console-consumer --bootstrap-server=localhost:9092 --topic=route.new-position --group=terminal
func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("error loading .env file")
	}
}

func main() {
	msgChan := make(chan *ckafka.Message)
	consumer := kafka.NewKafkaConsumer(msgChan)
	go consumer.Consume()
	for msg := range msgChan {
		fmt.Println(string(msg.Value))
		go kafka2.Produce(msg)
	}
}
