package io.devhands.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BasicStreams {
    public static void main(String[] args) throws IOException {
        var streamsProperties = System.getProperty("streams.properties");

        Properties props = new Properties();
        try (
                InputStream fis = streamsProperties == null
                        ? Thread.currentThread().getContextClassLoader().getResourceAsStream("streams.properties")
                        : new FileInputStream(streamsProperties)) {
            props.load(fis);
        }

        props.put("state.dir", Paths.get(System.getProperty("user.home"),"tmp", "kafka-streams").toString());
        props.put("application.id", "basic-streams");
        props.put("client.id", "basic-streams-client");
        props.put("consumer.group.instance.id", "consumer-id-1");

        final String sourceTopic = "streams-input";
        final String outputTopic = "streams-output";
        final String searchPrefix = "good-";

        StreamsBuilder builder = new StreamsBuilder();

        System.out.println("Consuming from topic [" + sourceTopic + "] and producing to [" + outputTopic + "] via " + props.get("bootstrap.servers"));

        // variant 1
        KStream<String, String> sourceStream = builder.stream(sourceTopic);
        props.put("default.key.serde", Serdes.String()
                .getClass()
                .getName());
        props.put("default.value.serde", Serdes.String()
                .getClass()
                .getName());

        // variant 2
        // KStream<String, String> sourceStream = builder.stream(sourceTopic,
        //     Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream
                .peek((key, value) -> System.out.println("In  >> key: " + key + ":\t" + value))
                .filter((key, value) -> value.contains(searchPrefix))
                .mapValues(value -> Long.parseLong(value.substring(value.indexOf("-") + 1)))
                .filter((key, value) -> value < 100)
                .peek((key, value) -> System.out.println("Out << key: " + key + ":\t" + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.
                        Long()));

        try (
                KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
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
