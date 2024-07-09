package rabbitmq

import (
	"log"
	"rabbitmq-consumer/config"
	"rabbitmq-consumer/internal/service"
	"rabbitmq-consumer/pkg/logger"
	"time"

	"github.com/streadway/amqp"
)

type AMQPConnection struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	done            chan bool
	url             string
	smsService      service.SMSService
	logInstance     *logger.Loggers
}

func NewAMQPConnection(cfg *config.Config, smsService service.SMSService, logInstance *logger.Loggers) *AMQPConnection {
	return &AMQPConnection{
		url:         cfg.RabbitMQ.URL,
		done:        make(chan bool),
		smsService:  smsService,
		logInstance: logInstance,
	}
}

func (c *AMQPConnection) Connect() error {
	var err error

	c.conn, err = amqp.Dial(c.url)
	if err != nil {
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return err
	}

	c.notifyConnClose = c.conn.NotifyClose(make(chan *amqp.Error))
	c.notifyChanClose = c.channel.NotifyClose(make(chan *amqp.Error))

	go c.handleReconnection()

	return nil
}

func (c *AMQPConnection) handleReconnection() {
	for {
		select {
		case <-c.notifyConnClose:
			log.Println("Connection closed. Reconnecting...")
			c.reconnect()
		case <-c.notifyChanClose:
			log.Println("Channel closed. Reconnecting...")
			c.reconnect()
		case <-c.done:
			return
		}
	}
}

func (c *AMQPConnection) reconnect() {
	for {
		err := c.Connect()
		if err == nil {
			log.Println("Reconnected to RabbitMQ")
			c.ConsumeMessages()
			return
		}

		log.Printf("Failed to reconnect: %v. Retrying in %v...", err, 5*time.Second)
		time.Sleep(5 * time.Second)
	}
}

func (c *AMQPConnection) Close() {
	c.done <- true
	if !c.conn.IsClosed() {
		c.channel.Close()
		c.conn.Close()
	}
}

func (c *AMQPConnection) ConsumeMessages() {
	err := c.channel.ExchangeDeclare(
		"extra.turkmentv", // name
		"direct",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to declare an exchange", "error", err)
		return
	}

	queue, err := c.channel.QueueDeclare(
		"extra.turkmentv", // name of the queue
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to declare a queue", "error", err)
		return
	}

	err = c.channel.QueueBind(
		queue.Name,        // queue name
		"",                // routing key
		"extra.turkmentv", // exchange
		false,
		nil,
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to bind queue to exchange", "error", err)
		return
	}

	msgs, err := c.channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
		return
	}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %v", string(d.Body))
			err := c.smsService.ProcessMessage(d.Body)
			if err != nil {
				c.logInstance.ErrorLogger.Error("Failed to process message", "error", err)
			}
		}
	}()
}
