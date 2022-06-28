package com.github.absinthminded.kafka.tutorial.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerDemoAssignSeek
{

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

		Properties properties = new Properties();
		String bootstrapServers = "127.0.0.1:9092";
		String offsetConfig = "earliest";
		String topic = "first_topic";

		//create consumer configs
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig); //earliest/latest/none

		KafkaConsumer consumer = new KafkaConsumer<String, String>(properties);

		//assign and seek are mostly used to replay data or fetch a specific message

		//assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		long offsetToReadFrom = 15L;
		consumer.assign(Arrays.asList(partitionToReadFrom));

		//seek
		consumer.seek(partitionToReadFrom, offsetToReadFrom);

		int numberOfMessagesToRead = 5;
		boolean keepOnReading = true;
		int numberOfReadMessages = 0;

		while (keepOnReading)
		{
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records)
			{
				numberOfReadMessages += 1;
				logger.info("Key: " + record.key() + ", value: " + record.value());
				logger.info("Partition: " + record.partition());
				logger.info("Offsets: " + record.offset());
				if (numberOfReadMessages >= numberOfMessagesToRead){
					keepOnReading = false;
					break;
				}
			}
		}

		logger.info("Exiting the application");
	}
}

