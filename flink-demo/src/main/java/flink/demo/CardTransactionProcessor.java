package flink.demo;

import flink.demo.deserializer.CardDeserializationSchema;
import flink.demo.deserializer.TransactionDeserializationSchema;
import flink.demo.filter.CardTransactionProcessorFunction;
import flink.demo.model.Card;
import flink.demo.model.Transaction;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class CardTransactionProcessor {

    public static final String ROCKSDB_PATH = "file:///flink-cdc-demo/rocksdb";
    public static final String KAFKA_BOOTSTRAP_SERVER = "kafka:9092";
    public static final String TOPIC_CARDS = "db.public.cards";
    public static final String TOPIC_TRANSACTIONS = "db.public.transactions";
    public static final String TOPIC_FILTERED_TRANSACTIONS = "filtered.transactions";
    public static final String CARD_CONSUMER_GROUP_ID = "flink-cards-consumer";
    public static final String TRANSACTIONS_CONSUMER_GROUP_ID = "flink-transactions-consumer";
    public static final String CARDS_STATE = "CardsState";


    // 1. Broadcast state descriptor
    public static final MapStateDescriptor<String, Card> CARD_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    CARDS_STATE,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(Card.class)
            );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // CPU * 0.8 = 8
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // 2. RocksDB state backend
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(ROCKSDB_PATH);
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 3. CARDS SOURCE
        KafkaSource<Card> cardsSource = KafkaSource.<Card>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
                .setTopics(TOPIC_CARDS)
                .setGroupId(CARD_CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CardDeserializationSchema())
                .build();

        // 4. Card stream
        DataStream<Card> cardsStream = env
                .fromSource(cardsSource, WatermarkStrategy.noWatermarks(), "Cards Source")
//                .setParallelism(5) // according to partition count
                .filter((FilterFunction<Card>) value -> value != null)
                .name("Cards Stream");

        // 5. Cards stream broadcasting
        BroadcastStream<Card> broadcastCardStream = cardsStream.broadcast(CARD_STATE_DESCRIPTOR);

        // 6. TRANSACTIONS SOURCE (Keyed Stream)
        KafkaSource<Transaction> transactionsSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
                .setTopics(TOPIC_TRANSACTIONS)
                .setGroupId(TRANSACTIONS_CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionDeserializationSchema())
                .build();

        // 7. Transactions stream
        DataStream<Transaction> transactionsStream = env
                .fromSource(transactionsSource, WatermarkStrategy.noWatermarks(), "Transactions Source")
                .name("Keyed Transactions Stream")
                .keyBy(Transaction::getCardId);

        // 8. Process Transactions with Card State
        DataStream<Transaction> filteredTransactions = transactionsStream
                .connect(broadcastCardStream)
                .process(new CardTransactionProcessorFunction(CARD_STATE_DESCRIPTOR))
                .name("Process Transactions with Card State");

        // 9. SINK
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(TOPIC_FILTERED_TRANSACTIONS)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        // 10. Send to sink
        filteredTransactions
                .map(Transaction::toString)
                .sinkTo(kafkaSink)
                .name("Filtered Transactions Sink");

        env.execute("CDC Cards & Transactions Filter");
    }
}