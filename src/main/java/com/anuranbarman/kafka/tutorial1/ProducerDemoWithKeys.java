package com.anuranbarman.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        String topic = "first_topic";
        for (int i=0;i<10;i++){
            String key = "id_"+i;
            String value = "Hello World "+i;
            final ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,key,value);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("Received New Metadata:\n"+
                                "Partition : "+recordMetadata.partition()+"\n"+
                                "Topic:"+recordMetadata.topic()+"\n"+
                                "Offset : "+recordMetadata.offset());
                    }else {
                        logger.error(e.getMessage());
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
