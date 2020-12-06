# KafkaTest
Kafka learning, Java, producer/consumer/confluent plattform, Avro data.

Project discription:
1. Payment class includes producer, could create different producer according to account Nr and source ("SWISH" or "CREDIT"), amount set to be random between 10 and 1000.
2. BalanceController includes consumer which could use for consuming the message and calculate the balance for every account.
3. Main method----create different thread for producer and run consumer.
4. To run it, please:
Unzip the file. 

Go into the folder KafkaTest:
    run “mvn clean”, then run “mvn package”

Copy the generated jar file to docker folder:
    cp ./target/KafkaTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar  ../docker/KafkaTest.jar

Go into the docker folder, run:
  docker build -t my-kafka-app .
  docker run -it --rm --name my-running-app my-kafka-app bash ./runTest.sh


