package com.github.absinthminded.kafka.tutorial;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;


public class ConsumerDemo {
	public static void main(String[] args)
	{
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

		Properties properties = new Properties();
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-fourth-application";
		String offsetConfig = "earliest";
		String subscriptionTopic = "first_topic";

		//create consumer configs
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig); //earliest/latest/none

		//create consumer
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

		//subscribe consumer to the topic
		kafkaConsumer.subscribe(Collections.singleton(subscriptionTopic));
		//Arrays.asList("first", "second", "third")

		//poll the new data
		while (true){
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records)
			{
				logger.info("Key: " + record.key() + ", value: " + record.value());
				logger.info("Partition: " + record.partition());
				logger.info("Offsets: " + record.offset());
			}
		}
	}

}
