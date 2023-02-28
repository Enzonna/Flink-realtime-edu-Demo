package com.atguigu.edu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.app.func.MyBroadcastFunction;
import com.atguigu.edu.realtime.app.func.MyPhoenixSink;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.util.EnvUtil;
import com.atguigu.edu.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备及状态后端设置
        StreamExecutionEnvironment env = EnvUtil.getStreamTableEnvironment(4);

        // TODO 2. 读取业务主流
        String topic = "topic_db";
        String groupId = "dim_sink_app";
        DataStreamSource<String> eduDS = env.fromSource(KafkaUtil.getKafkaConsumer(topic, groupId),
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        // TODO 3. 主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = eduDS.map(JSON::parseObject);

        // TODO 4. 主流 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                jsonObj ->
                {
                    try {
                        jsonObj.getJSONObject("data");
                        if (jsonObj.getString("type").equals("bootstrap-start")
                                || jsonObj.getString("type").equals("bootstrap-complete")) {
                            return false;
                        }
                        return true;
                    } catch (JSONException jsonException) {
                        return false;
                    }
                });

        // TODO 5. FlinkCDC 读取配置流并广播流
        // 5.1 FlinkCDC 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("edu_config") // set captured database
                .tableList("edu_config.table_process") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // 5.2 封装为流
        DataStreamSource<String> mysqlDSSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        // 5.3 广播配置流
        MapStateDescriptor<String, DimTableProcess> tableConfigDescriptor = new MapStateDescriptor<String, DimTableProcess>("table-process-state", String.class, DimTableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDSSource.broadcast(tableConfigDescriptor);

        // TODO 6. 连接流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastDS);

        // TODO 7. 处理维度表数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(
                new MyBroadcastFunction(tableConfigDescriptor)
        );

        // TODO 8. 将数据写入 Phoenix 表
        dimDS.addSink(new MyPhoenixSink());

        env.execute();
    }
}
