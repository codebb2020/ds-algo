package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TotalExposure {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "total-exposure-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> trades = builder.stream("trades");

        // Set key to title and value to ticket value
        trades.map((k, v) -> {
            List<String> tardeList = Arrays.asList( v.split(","));
            System.out.println( "=============" + v );
            return new KeyValue<>( tardeList.get(0), Double.parseDouble(tardeList.get(2)));
        })
                // Group by title
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                // Apply SUM aggregation
                .reduce(Double::sum)
                // Write to stream specified by outputTopic
                .toStream().to("trades-output", Produced.with(Serdes.String(), Serdes.Double()));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        System.out.println( streams.toString() );

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

