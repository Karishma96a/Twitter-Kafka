package com.twitter.kafka;

import com.twitter.kafka.consumer.KafkaConsumerMongoDb;
import com.twitter.kafka.consumer.TwitterConsumer;
import com.twitter.kafka.producer.TwitterProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class TwitterStreamLifeCycleService {

    @Autowired
    private TwitterProducer twitterProducer;

    @Autowired
    private TwitterConsumer twitterConsumer;

    @Autowired
    private KafkaConsumerMongoDb kafkaConsumerMongoDb;

    public void startLifecycleServices() {
        final ExecutorService executor = Executors.newFixedThreadPool(3);
        executor.submit(() -> twitterProducer.startTwitterProducer());
        executor.submit(() -> twitterConsumer.startTwitterConsumer());
        executor.submit(() -> kafkaConsumerMongoDb.startMongoDbConsumer());
    }
}
