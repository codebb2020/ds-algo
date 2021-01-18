package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class Producer {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" );
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>( config );

        String fileName = "lines.txt";
        String topicName = "trades";
        //read file
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach( line -> {
                producer.send( new ProducerRecord<>( topicName, line ), (recordMetadata, e) -> {
                    if( e == null ){
                        System.out.println( "Topic: " + recordMetadata.topic()
                                            + "\tPartition: " + recordMetadata.partition()
                                            + "\tOffset: " + recordMetadata.offset() );
                    } else {
                        System.out.println( "data not written" );
                    }
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.flush();
        producer.close();
    }
}
