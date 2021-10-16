package com.twitter.kafka;

import com.twitter.kafka.consumer.KafkaConsumerMongoDb;
import com.twitter.kafka.consumer.TwitterConsumer;
import com.twitter.kafka.producer.TwitterProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@SpringBootApplication
public class KafkaApplication implements CommandLineRunner {

	@Autowired
	private TwitterStreamLifeCycleService twitterStreamLifeCycleService;

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		twitterStreamLifeCycleService.startLifecycleServices();
	}
}
