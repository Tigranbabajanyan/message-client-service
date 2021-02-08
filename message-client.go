package micro_connect_service

import (
	"github.com/streadway/amqp"
	"log"
)

type MessageClient interface {
	PublishOnQueue(message []byte, queueName string)
	SubscribeToQueue(queueName string, consumerName string, handleFunc func(delivery amqp.Delivery))
}

type messageClient struct {
	channel *amqp.Channel
}

func NewMessageClient(rabbitMQ *amqp.Connection) MessageClient {

	channel, err := rabbitMQ.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "Failed to open a channel", err)
	}

	defer channel.Close()

	return messageClient{channel: channel}
}

func (client messageClient) PublishOnQueue(message []byte, queueName string) {
	queue, err := client.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		log.Fatalf("%s: %s", "Failed to declare a queue", err)
		return
	}

	err = client.channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})

	if err != nil {
		println(err)
	}
}

func (client messageClient) SubscribeToQueue(queueName string, consumerName string, handleFunc func(delivery amqp.Delivery)) {
	msgs, err := client.channel.Consume(
		queueName,    // queue
		consumerName, // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)

	if err != nil {
		log.Fatalf("%s: %s", "Failed to register a consumer", err)
	}

	go func(handleFunc func(delivery amqp.Delivery)) {
		for d := range msgs {
			handleFunc(d)
		}
	}(handleFunc)
}
