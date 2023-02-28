package com.atguigu.edu.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DwdTableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BaseDBBroadcastFunc extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 广播状态描述器
    private MapStateDescriptor<String, DwdTableProcess> tableConfigDescriptor;

    // 预加载配置
    private HashMap<String, DwdTableProcess> configMap = new HashMap<>();

    public BaseDBBroadcastFunc(MapStateDescriptor<String, DwdTableProcess> tableConfigDescriptor) {
        this.tableConfigDescriptor = tableConfigDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?" +
                "user=root&password=000000&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        String sql = "select * from edu_config.dwd_table_process";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        ResultSet rs = preparedStatement.executeQuery();

        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            JSONObject jsonValue = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = rs.getString(i);
                jsonValue.put(columnName, columnValue);
            }

            String key = jsonValue.getString("source_table");
            configMap.put(key, jsonValue.toJavaObject(DwdTableProcess.class));
        }

        rs.close();
        preparedStatement.close();
        conn.close();
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, DwdTableProcess> tableConfigState = readOnlyContext.getBroadcastState(tableConfigDescriptor);

        // 获取配置信息
        String sourceTable = jsonObj.getString("table");

        // 从广播状态中获取配置信息
        DwdTableProcess tableConfig = tableConfigState.get(sourceTable);
        // 如果没有获取到则尝试从初始化对象中获取配置信息
        if (tableConfig == null) {
            tableConfig = configMap.get(sourceTable);
        }

        // 过滤不参与动态分流的数据
        if (tableConfig != null) {
            String sourceType = tableConfig.getSourceType();
            String type = jsonObj.getString("type");
            if (sourceType.equals(type)) {
                JSONObject data = jsonObj.getJSONObject("data");
                String sinkTable = tableConfig.getSinkTable();
                // 根据 sinkColumns 过滤字段
                String sinkColumns = tableConfig.getSinkColumns();
                filterColumns(data, sinkColumns);

                // 将目标表名加入到主流数据中
                data.put("sinkTable", sinkTable);

                // 将时间戳加到主流数据中
                Long ts = jsonObj.getLong("ts");
                data.put("ts", ts);

                out.collect(data);
            }

        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> dataEntries = data.entrySet();
        dataEntries.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {

        JSONObject jsonObj = JSON.parseObject(jsonStr);

        BroadcastState<String, DwdTableProcess> tableConfigState = context.getBroadcastState(tableConfigDescriptor);

        String op = jsonObj.getString("op");

        // 若操作类型是删除，则清除广播状态中的对应值
        if ("d".equals(op)) {
            DwdTableProcess before = jsonObj.getObject("before", DwdTableProcess.class);
            String sourceTable = before.getSourceTable();

            // 清除状态和预加载 Map 中的配置信息
            tableConfigState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            DwdTableProcess after = jsonObj.getObject("after", DwdTableProcess.class);
            String sourceTable = after.getSourceTable();

            // 将状态更新到广播状态和预加载 Map 中
            tableConfigState.put(sourceTable, after);
            configMap.put(sourceTable, after);
        }
    }
}

