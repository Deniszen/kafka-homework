package ru.otus.hw;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class EventCountService {
    public static final String TOPIC = "events";
    public static final String STORE = "events-session-store";
    public static final Duration SESSION_GAP = Duration.ofMinutes(5);
    private static final String HOST = "localhost:9091";

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<Long> longSerde = Serdes.Long();

    private KafkaStreams kafkaStreams;
    private ReadOnlySessionStore<String, Long> store;

    @PostConstruct
    public void start() {
        createTopic();

        var builder = new StreamsBuilder();
        builder.stream(TOPIC, Consumed.with(stringSerde, stringSerde))
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(SESSION_GAP))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde))
                .toStream()
                .filter((window, count) -> count != null) // null приходит на закрытые (слитые) сессии
                .foreach((window, count) -> log.info("key={} session=[{} .. {}] count={}",
                        window.key(), window.window().startTime(), window.window().endTime(), count));

        kafkaStreams = new KafkaStreams(builder.build(), new StreamsConfig(Map.of(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOST,
                StreamsConfig.APPLICATION_ID_CONFIG, "events-session-count",
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000,
                StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0,
                StreamsConfig.STATE_DIR_CONFIG, System.getProperty("user.dir") + "/kafka-state")));

        kafkaStreams.start();
        log.info("Streams started, topic={}, session gap={}", TOPIC, SESSION_GAP);
    }

    public Map<String, Long> count(String key) {
        var result = new LinkedHashMap<String, Long>();
        try (var it = store().fetch(key)) {
            it.forEachRemaining(kv -> result.put(
                    kv.key.window().startTime() + " .. " + kv.key.window().endTime(), kv.value));
        }
        return result;
    }

    private ReadOnlySessionStore<String, Long> store() {
        if (store == null) {
            store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(STORE,
                    QueryableStoreTypes.sessionStore()));
        }
        return store;
    }

    private void createTopic() {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
            log.info("Topic {} created", TOPIC);
        } catch (Exception e) {
            log.info("Topic {} not created: {}", TOPIC, e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        kafkaStreams.close();
    }
}
