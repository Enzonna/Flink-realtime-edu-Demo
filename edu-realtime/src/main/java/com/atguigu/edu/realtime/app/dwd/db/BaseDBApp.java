package com.atguigu.edu.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.BaseDBBroadcastFunc;
import com.atguigu.edu.realtime.bean.DwdTableProcess;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 读取业务主流
        String topic = "topic_db";
        String groupId = "base_db_app";
        DataStreamSource<String> eduDS = env.fromSource(KafkaUtil.getKafkaConsumer(topic, groupId),
                WatermarkStrategy.noWatermarks(), "base_db_source");

        // TODO 3. 主流 ETL
        SingleOutputStreamOperator<String> filterDS = eduDS.filter(
                jsonStr ->
                {
                    try {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        jsonObj.getJSONObject("data");
                        return !jsonObj.getString("type").equals("bootstrap-start")
                                && !jsonObj.getString("type").equals("bootstrap-complete")
                                && !jsonObj.getString("type").equals("bootstrap-insert");
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        return false;
                    }
                });

        // TODO 4. 主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = filterDS.map(JSON::parseObject);

        // TODO 5. FlinkCDC 读取配置流并广播流
        // 5.1 FlinkCDC 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("edu_config") // set captured database
                .tableList("edu_config.dwd_table_process") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // 5.2 封装为流
        DataStreamSource<String> mysqlDSSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "base-db-mysql-source")
                .setParallelism(1);

        // 5.3 广播配置流
        MapStateDescriptor<String, DwdTableProcess> tableConfigDescriptor =
                new MapStateDescriptor<String, DwdTableProcess>("dwd-table-process-state", String.class, DwdTableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDSSource.broadcast(tableConfigDescriptor);

        // TODO 6. 连接流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonDS.connect(broadcastDS);

        // TODO 7. 处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(
                new BaseDBBroadcastFunc(tableConfigDescriptor)
        );

        // TODO 8. 将数据写入 Kafka
        dimDS.sinkTo(KafkaUtil.<JSONObject>getProducerBySchema(
                new KafkaRecordSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                        String topic = element.getString("sinkTable");
                        // sinkTable 字段不需要写出，清除
                        element.remove("sinkTable");

                        return new ProducerRecord<byte[], byte[]>(topic, element.toJSONString().getBytes());
                    }
                }, "base_db_app_trans"
        ));

        env.execute();
    }
}
