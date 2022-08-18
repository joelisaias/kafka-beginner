package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Consumer");

        String bootstrapServer ="127.0.0.1:9092";
        String groupId ="my-second-aplication";
        String topic ="demo_java";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //Subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data

        while (true){
            log.info("Polling");
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,String> record: records) {

                log.info("key: "+ record.key() + ", Value: "+record.value());
                log.info("Partition: "+ record.partition() + ", Offset: "+ record.offset());

            }

        }

    }
}