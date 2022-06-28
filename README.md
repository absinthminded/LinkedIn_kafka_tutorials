# LinkedIn_kafka_tutorials
LinkedIn learning Kafka for Java introduction course. Basic principles of working with messaging, installing and setting up.

Useful commands for terminal to launch Kafka services:

	1- Run zookeeper zookeeper-server-start config/zookeeper.properties
	2- Run kafka kafka-server-start config/server.properties 
	3- Run kafka-topics --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1
		3.1 List of topics kafka-topics --bootstrap-server localhost:9092 --list
		3.2 topic description kafka-topics --bootstrap-server localhost:9092 --topic first_topic --describe
		3.3 delete topic kafka-topics --bootstrap-server localhost:9092 --topic second_topic --delete 
	4- New producer : kafka-console-producer --bootstrap-server localhost:9092 --broker-list localhost:9092 --topic first_topic
	5- Consumer kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --from-beginning //from beginning of topic
	6- Groups kafka-consumer-groups --bootstrap-server localhost:9092 --list 

