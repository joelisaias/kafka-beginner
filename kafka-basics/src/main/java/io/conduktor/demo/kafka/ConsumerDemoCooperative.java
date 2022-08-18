package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Consumer");

        String bootstrapServer ="127.0.0.1:9092";
        String groupId ="my-third-aplication";
        String topic ="demo_java";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //Create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //get a reference to a current thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();
                //join the main thread to allow execution of the code in the main thread
                try {
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }

            }
        });
    try {
        //Subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data

        while (true){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String,String> record: records) {

                log.info("key: "+ record.key() + ", Value: "+record.value());
                log.info("Partition: "+ record.partition() + ", Offset: "+ record.offset());

            }

        }

    }catch (WakeupException e){
        log.info("Wake up exception!");
    }catch (Exception e){
        log.error("unexpected exception");
    }finally {
        consumer.close();
        log.info("the consumer in now gracefully closed");
    }

    }
}
