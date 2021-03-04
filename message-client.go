package micro_connect_service

import (
	"github.com/streadway/amqp"
	"log"
)

type MessageClient interface {
	PublishOnQueue(message []byte, queueName string) error
	PublishOnExchange(message []byte, exchange *ExchangeDeclare, routingKey string) error
	SubscribeToQueue(queueName string, consumerName string, handleFunc func(delivery amqp.Delivery)) error
}

type messageClient struct {
	channel *amqp.Channel
}

func NewMessageClient(rabbitMQ *amqp.Connection) MessageClient {

	channel, err := rabbitMQ.Channel()
	if err != nil {
		log.Fatalf("%s: %s", "Failed to open a channel", err)
	}

	return messageClient{channel: channel}
}

func (client messageClient) PublishOnQueue(message []byte, queueName string) error {
	queue, err := client.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		return err
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

	return err
}

func (client messageClient) PublishOnExchange(message []byte, exchange *ExchangeDeclare, routingKey string) error {
	//err := client.channel.ExchangeDeclare(
	//	exchangeName, // name
	//	"topic",      // type
	//	false,        // durable
	//	false,        // auto-deleted
	//	false,        // internal
	//	false,        // no-wait
	//	nil,          // arguments
	//)
	err := client.channel.ExchangeDeclare(
		exchange.Name,       // name
		exchange.Kind,       // type
		exchange.Durable,    // durable
		exchange.AutoDelete, // auto-deleted
		exchange.Internal,   // internal
		exchange.noWait,     // no-wait
		nil,                 // arguments
	)

	if err != nil {
		return err
	}

	err = client.channel.Publish(
		exchange.Name, // exchange
		routingKey,    // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	return err
}

func (client messageClient) SubscribeToQueue(queueName string, consumerName string, handleFunc func(delivery amqp.Delivery)) error {
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
		return err
	}

	go func(handleFunc func(delivery amqp.Delivery)) {
		for d := range msgs {
			handleFunc(d)
		}
	}(handleFunc)

	return nil
}
