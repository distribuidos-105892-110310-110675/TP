import pika

RABBITMQ_HOST = "rabbitmq-dev"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"


class TestRabbitMQMessageMiddlewareExchange:
    def test_working_exchange_communication_1_to_1(self) -> None:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="rabbitmq-dev")
        )
        channel = connection.channel()

        channel.queue_declare(queue="hello")

        channel.basic_publish(exchange="", routing_key="hello", body="Hello World!")
        print(" [x] Sent 'Hello World!'")

        connection.close()

        # receive

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=5672,
                credentials=pika.PlainCredentials(username="guest", password="guest"),
            )
        )
        channel = connection.channel()

        channel.queue_declare(queue="hello")

        def callback(
            channel: pika.adapters.blocking_connection.BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            print(f" [x] Received {str(body)}")
            assert False

        channel.basic_consume(
            queue="hello", on_message_callback=callback, auto_ack=True
        )

        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
