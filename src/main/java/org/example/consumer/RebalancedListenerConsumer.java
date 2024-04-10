package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.example.consumer.SimpleConsumer.*;

public class RebalancedListenerConsumer {
    private final static Logger logger = LoggerFactory.getLogger(RebalancedListenerConsumer.class);
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record: {}", record);
            }
        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener  {
        private final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            logger.info("partitions revoked: {}", collection.toString());
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            logger.info("partitions assigned: {}", collection.toString());
        }
    }
}
