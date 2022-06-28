package com.github.absinthminded.kafka.tutorial.producers;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerDemoWithCallback {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

		String bootstrapServers = "127.0.0.1:9092";

		Properties properties = new Properties();
		properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int i = 0; i < 10; i++)
		{
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world " + i);

			producer.send(producerRecord, new Callback()
			{
				@Override
				public void onCompletion(RecordMetadata recordMetadata, Exception e)
				{
					if (e == null){
						logger.info("Recieved the new metadata. \n" +
								"Topic: " + recordMetadata.topic() + "\n" +
								"Offset: " + recordMetadata.offset() + "\n" +
								"Partition: " + recordMetadata.partition() + "\n" +
								"Timestamp: " + recordMetadata.timestamp());
					}
					else {
						logger.error("Error while producing ", e);
					}
				}
			});
		}

		producer.flush();
		producer.close();
	}

}
