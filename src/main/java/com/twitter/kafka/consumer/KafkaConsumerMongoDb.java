package com.twitter.kafka.consumer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.twitter.kafka.config.KafkaConfig;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@NoArgsConstructor
@Component
public class KafkaConsumerMongoDb {

    public void startMongoDbConsumer() {
        final Logger logger = LoggerFactory.getLogger(KafkaConsumerMongoDb.class.getName());
        final MongoCollection<Document> collection = createSink();

        final KafkaConsumer<String, String> consumer = createConsumer(KafkaConfig.TOPIC);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                Document doc = Document.parse(record.value());
                collection.insertOne(doc);
            }
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                logger.info("commit failed", e);
            }
        }
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.GROUP_ID);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;

    }

    public static MongoCollection<Document> createSink() {

        final ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017/admin");
        final MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();

        final MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        final MongoDatabase database = mongoClient.getDatabase("twitter");

        final MongoCollection<Document> collection = database.getCollection("twitter_tweets");
        System.out.println("Collection myCollection selected successfully");
        return collection;
    }
}