package io.devhands.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KTableStreams {
    public static void main(String[] args) throws IOException {

        var streamsProperties = System.getProperty("streams.properties");

        Properties props = new Properties();
        try (
                InputStream fis = streamsProperties == null
                        ? Thread.currentThread().getContextClassLoader().getResourceAsStream("streams.properties")
                        : new FileInputStream(streamsProperties)) {
            props.load(fis);
        }
        // props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-example");
        props.put("application.id", "ktable-example"); // consumer group

        // props.put(StreamsConfig.consumerPrefix("group.instance.id"), "consumer-id-1");
        props.put("consumer.group.instance.id", "consumer-id-1");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = props.getProperty("ktable.input.topic");
        final String outputTopic = props.getProperty("ktable.output.topic");

        final String filterPrefix = "good-";

        // Crate a table with the StreamBuilder from above and use the table method
        // along with the inputTopic create a Materialized instance and name the store
        // and provide a Serdes for the key and the value  HINT: Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as
        // then use two methods to specify the key and value serde

        System.out.println("Consuming '" + filterPrefix + "*' from topic [" + inputTopic + "] and producing to [" + outputTopic + "] via " + props.get("bootstrap.servers"));

        // props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        /* Variant 1 */
        // KTable<String, String> table = builder.table(inputTopic);
        // props.put("default.key.serde", Serdes.String().getClass().getName());
        // props.put("default.value.serde", Serdes.String().getClass().getName());

        /* Variant 2 */
        Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized =
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())

                        .withCachingDisabled()
                // .withCachingEnabled()
                // .withRetention(Duration.ofSeconds(5))
                ;



        KTable<String, String> table = builder.table(inputTopic, materialized);

        table
                .filter((key, value) -> value.contains(filterPrefix))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))

                .toStream()
                .peek((key, value) -> System.out.println("Out << key: " + key + ":\t" + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))
        ;

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
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
