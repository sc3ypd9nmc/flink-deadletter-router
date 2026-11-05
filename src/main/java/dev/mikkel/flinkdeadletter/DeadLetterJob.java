package dev.mikkel.flinkdeadletter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Entry point: raw orders in, validated orders forward, rejects to quarantine.
 *
 * <p>Delivery: exactly-once checkpoints, at-least-once idempotent sinks. A
 * restored checkpoint can repeat records, so the valid sink dedupes on
 * {@link DeadLetterEnvelope#dedupeKey()}. See README for the replay procedure.
 */
public final class DeadLetterJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DeadLetterJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Duration.ofSeconds(10)));

        String bootstrap = params.getRequired("bootstrap");
        String sourceTopic = params.get("source-topic", "orders.raw");
        String sinkTopic = params.get("sink-topic", "orders.valid");

        KafkaSource<OrderEvent> source = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(sourceTopic)
                .setGroupId(params.get("group", "flink-deadletter-router"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets())
                .setDeserializer(new EventDeserializer())
                .build();

        DataStream<OrderEvent> raw =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders.raw");

        SingleOutputStreamOperator<String> valid =
                raw.process(new QuarantineRouter()).name("validate-order").uid("validate-order");

        valid.sinkTo(kafkaSink(bootstrap, sinkTopic)).name("orders.valid");
        valid.getSideOutput(QuarantineRouter.DEAD_LETTERS)
                .map(DeadLetterJob::toJson)
                .returns(TypeInformation.of(String.class))
                .sinkTo(kafkaSink(bootstrap, params.get("quarantine-topic", "orders.quarantine")))
                .name("orders.quarantine");

        env.execute("deadletter-router");
    }

    private static KafkaSink<String> kafkaSink(String bootstrap, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty("enable.idempotence", "true")
                .build();
    }

    private static String toJson(DeadLetterEnvelope envelope) {
        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("envelope serialisation failed: " + envelope.dedupeKey(), e);
        }
    }

    /** Raw bytes plus the coordinates the quarantine record must carry. */
    public record OrderEvent(String topic, int partition, long offset, String raw) {
    }

    /** Keeps Kafka metadata alongside the payload; the plain string value loses it. */
    public static final class EventDeserializer implements KafkaRecordDeserializer<OrderEvent> {

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<OrderEvent> out) {
            String raw = record.value() == null
                    ? null
                    : new String(record.value(), StandardCharsets.UTF_8);
            out.collect(new OrderEvent(record.topic(), record.partition(), record.offset(), raw));
        }

        @Override
        public TypeInformation<OrderEvent> getProducedType() {
            return TypeInformation.of(OrderEvent.class);
        }
    }
}
