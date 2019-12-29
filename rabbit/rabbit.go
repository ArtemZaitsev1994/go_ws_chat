package rabbit

import (
	"github.com/streadway/amqp"
	"fmt"
)

var rabbit_chat_queue *amqp.Channel
func FailOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
	}
}

func CreateRabbitQueue(queue_name string) (amqp.Channel, string) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	FailOnError(err, "Failed to connect")
	defer conn.Close()

	rabbit_chat_queue, err := conn.Channel()
	FailOnError(err, "Fail to open a channel")
	defer rabbit_chat_queue.Close()

	q, err := rabbit_chat_queue.QueueDeclare(
		queue_name,
		false,
		true,
		false,
		false,
		nil,
	)
	FailOnError(err, "Failed to declare a queue")
	return *rabbit_chat_queue, q.Name

}
