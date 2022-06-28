package com.github.absinthminded.kafka.tutorial;

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


public class ConsumerDemoWithThread
{
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}

	private ConsumerDemoWithThread(){

	}

	private void run(){
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-sixth-application";
		String offsetConfig = "earliest";
		String subscriptionTopic = "first_topic";

		//latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);

		logger.info("Creating consumer thread");
		//create consumer runnable
		Runnable consumerRunnable
				= new ConsumerRunnable(latch, bootstrapServers, groupId, subscriptionTopic, offsetConfig);

		//start the thread
		Thread myThread = new Thread(consumerRunnable);
		myThread.start();

		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Caught shutdown hook");
			((ConsumerRunnable) consumerRunnable).shutdown();

			try
			{
				latch.await();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));


		try
		{
			latch.await();
		}
		catch (InterruptedException e)
		{
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}

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
}
