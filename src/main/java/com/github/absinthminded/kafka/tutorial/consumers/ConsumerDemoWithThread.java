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


public class ConsumerDemoWithThread
{
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}

	private void run(){
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-seventh-application";
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
}
