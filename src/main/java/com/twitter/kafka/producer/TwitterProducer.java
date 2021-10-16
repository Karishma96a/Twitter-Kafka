package com.twitter.kafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.kafka.config.KafkaConfig;
import com.twitter.kafka.config.TwitterConfig;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

@Slf4j
@NoArgsConstructor
@Component
public class TwitterProducer {

    private Client client;
    private KafkaProducer<String, String> producer;
    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(30);
    private final List<String> trackTerms = Lists.newArrayList("coronavirus");

    public void startTwitterProducer() {
        log.info("Setting up");

        connectTwitterClient();
        producer = createKafkaProducer();
        addShutDownHook();
        sendTweetsToKafka();
        log.info("\n Application End");
    }

    private void connectTwitterClient() {
        client = createTwitterClient(msgQueue);
        client.connect();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        final Properties prop = getProperties();
        return new KafkaProducer<>(prop);
    }

    public Client createTwitterClient(final BlockingQueue<String> msgQueue) {
        final Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        final StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();
        hbEndpoint.trackTerms(trackTerms);

        // Twitter API and tokens
        final Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEYS, TwitterConfig.CONSUMER_SECRETS, TwitterConfig.TOKEN, TwitterConfig.SECRET);

        final ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client")
                .hosts(hoseBirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hbEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }

    private void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Application is not stopping!");
            client.stop();
            log.info("Closing Producer");
            producer.close();
            log.info("Finished closing");
        }));
    }

    private void sendTweetsToKafka() {
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                log.info(msg);
                producer.send(new ProducerRecord<>(KafkaConfig.TOPIC, null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        log.error("Some error OR something bad happened", e);
                    }
                });
            }
        }
    }

    private Properties getProperties() {
        final Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVERS);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConfig.ACKS_CONFIG);
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaConfig.RETRIES_CONFIG);
        prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaConfig.MAX_IN_FLIGHT_CONN);

        // Additional settings for high throughput producer
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConfig.COMPRESSION_TYPE);
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaConfig.LINGER_CONFIG);
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConfig.BATCH_SIZE);
        return prop;
    }
}