package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(brokersCSV, topic string) *Publisher {
	brokers := strings.Split(brokersCSV, ",")
	return &Publisher{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			RequiredAcks: kafkago.RequireOne,
			Balancer:     &kafkago.LeastBytes{},
			Async:        false,
		},
	}
}

func EnsureTopic(ctx context.Context, brokersCSV, topic string, partitions, replicationFactor int) error {
	brokers := strings.Split(brokersCSV, ",")
	if len(brokers) == 0 || brokers[0] == "" {
		return errors.New("no kafka brokers configured")
	}

	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("lookup kafka controller: %w", err)
	}

	controllerAddress := net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port))
	controllerConn, err := kafkago.DialContext(ctx, "tcp", controllerAddress)
	if err != nil {
		return fmt.Errorf("dial kafka controller: %w", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil && !strings.Contains(err.Error(), "Topic with this name already exists") {
		return fmt.Errorf("create kafka topic: %w", err)
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

func (p *Publisher) PublishJSON(ctx context.Context, key string, payload any) error {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal kafka payload: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(key),
		Value: bytes,
		Time:  time.Now().UTC(),
	})
}

type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(brokersCSV, topic, groupID string) *Consumer {
	brokers := strings.Split(brokersCSV, ",")
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
			MaxWait: 3 * time.Second,
		}),
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) Consume(ctx context.Context, handler func(context.Context, []byte) error) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		if err := handler(ctx, message.Value); err != nil {
			return err
		}

		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit kafka message: %w", err)
		}
	}
}
