package com.atguigu.edu.realtime.util;

import java.io.IOException;
import java.util.Properties;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaUtil {
    static String BOOTSTRAP_SERVERS = "hadoop102:9092, hadoop103:9092, hadoop104:9092";

    public static KafkaSink<String> getKafkaProducer(String topic, String transId) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                //指定生产的精准一次性
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果生产的精准一次性消费   那么每一个流数据在做向kakfa写入的时候  都会生成一个transId，默认名字生成规则相同，会冲突，我们这里指定前缀进行区分
                .setTransactionalIdPrefix(transId)
                .build();
    }

    public static KafkaSource<String> getKafkaConsumer(String topic, String groupId) {

        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null && bytes.length != 0) {
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
    }

    /**
     * 自定义序列化器获取 FlinkKafkaProducer
     * @param kafkaRecordSerializationSchema 自义定 Kafka 序列化器
     * @param <T> 流中元素的数据类型
     * @return 返回的 FlinkKafkaProducer
     */
    public static <T> KafkaSink<T> getProducerBySchema(KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema, String transId) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                //指定生产的精准一次性
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果生产的精准一次性消费   那么每一个流数据在做向kakfa写入的时候  都会生成一个transId，默认名字生成规则相同，会冲突，我们这里指定前缀进行区分
                .setTransactionalIdPrefix(transId)
                .build();
    }

    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets', " +
                " 'properties.auto.offset.reset' = 'earliest')";
    }

    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

    public static void createTopicDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + getKafkaDDL("topic_db", groupId));
    }
}
