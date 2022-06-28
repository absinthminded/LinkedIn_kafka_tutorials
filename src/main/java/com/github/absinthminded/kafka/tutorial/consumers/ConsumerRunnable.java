package com.github.absinthminded.kafka.tutorial.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerRunnable implements Runnable{

	private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

	private CountDownLatch latch;
	private KafkaConsumer<String, String> kafkaConsumer;


	public ConsumerRunnable(CountDownLatch latch,
			String bootstrapServers,
			String groupId,
			String subscriptionTopic,
			String offsetConfig){

		Properties properties = new Properties();

		//create consumer configs
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig); //earliest/latest/none

		this.latch = latch;
		kafkaConsumer = new KafkaConsumer<String, String>(properties);

		//subscribe consumer to the topic
		kafkaConsumer.subscribe(Collections.singleton(subscriptionTopic));
		//Arrays.asList("first", "second", "third")

	}

	@Override
	public void run()
	{
		try
		{
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
		} catch (WakeupException wakeupException) {
			logger.info("Received shutdown signal");
		} finally {
			kafkaConsumer.close();
			//tell main code we're done with consumer
			latch.countDown();
		}

	}

	public void shutdown(){
		//to interrupt consumer.poll -> throws WakeUpException
		kafkaConsumer.wakeup();
	}
}
