# KafkaTest
Kafka learning, Java, producer/consumer/confluent plattform, Avro data.

Project discription:
1. Payment class includes producer, could create different producer according to account Nr and source ("SWISH" or "CREDIT"), amount set to be random between 10 and 1000.
2. BalanceController includes consumer which could use for consuming the message and calculate the balance for every account.
3. Main method----create different thread for producer and run consumer.
