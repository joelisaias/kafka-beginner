package io.conduktor.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Producer");

        // CREATE PRODUCER PROPERTIES
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //CREATE THE PRODUCER
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


        IntStream.range(1,10)
                .forEach((i)->{

                    String topic = "demo_java";
                    String value = "messaje with key "+i;
                    String key = "id_"+i;

                    sendMessage(producer,topic,key,value);
                }

                );


        //flush data - synchronous
        producer.flush();

        //flush and close
        producer.close();

    }

    public static void sendMessage(KafkaProducer<String,String> producer,String topic,String key,String value){
        //Create a producer record
        ProducerRecord<String,String> producerRecord=
                new ProducerRecord<>(topic,key,value);
        //send data - asynchronous
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                //executes every time a record is successfully sent or an exception is thrown
                if(exception == null){
                    //the record was successfully sent
                    log.info("Received new metadata/ \n" +
                            "Topic: "+ metadata.topic()+"\n" +
                            "Key: "+ producerRecord.key()+"\n" +
                            "Partition: "+ metadata.partition()+"\n" +
                            "Offset: "+ metadata.offset()+"\n" +
                            "Timestamp: "+ metadata.timestamp()+"\n"
                    );
                }else{
                    log.error("Error while producing "+ exception);
                }
            }
        });
    }
}
