package io.devhands.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StatefulStreams {
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static String timeFormat(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
        ).format(formatter);
    }

    public static void main(String[] args) throws IOException {
        var streamsProperties = System.getProperty("streams.properties");

        Properties props = new Properties();
        try (
                InputStream fis = streamsProperties == null
                        ? Thread.currentThread().getContextClassLoader().getResourceAsStream("streams.properties")
                        : new FileInputStream(streamsProperties)) {
            props.load(fis);
        }

        props.put("application.id", "stateful-app-1"); // group
        props.put("consumer.group.instance.id", "consumer-id-1");

        props.put("commit.interval.ms", "2500");
        props.put("state.dir", "data");

        final String sourceTopic = "streams-input";
        final String outputTopic = "streams-agg-output";
        final String searchPrefix = "good-";

        StreamsBuilder builder = new StreamsBuilder();

        System.out.println("Consuming from topic [" + sourceTopic + "] and producing to [" + outputTopic + "]");

        KStream<String, String> sourceStream = builder.stream(sourceTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        // ...

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(1));
                shutdownLatch.countDown();
            }));
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
