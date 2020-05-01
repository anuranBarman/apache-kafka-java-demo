package com.anuranbarman.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    private CountDownLatch latch;
    private Logger logger;
    private KafkaConsumer<String,String> consumer;

    public ConsumerRunnable(String groupId, CountDownLatch latch){
        logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("first_topic"));

        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String,String> record:records){
                    logger.info("Key : "+record.key()+", Value: "+record.value());
                    logger.info("Partition : "+record.partition()+", Offset: "+record.offset());
                }
            }
        }catch (WakeupException e){
            logger.info("Received Shutdown Signal !!");
        }finally {
            consumer.close();
            latch.countDown();
        }
    }

    void shutdown(){
        consumer.wakeup();
    }

}
