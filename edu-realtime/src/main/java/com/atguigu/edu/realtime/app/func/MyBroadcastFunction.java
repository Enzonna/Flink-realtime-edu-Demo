package com.atguigu.edu.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.bean.DimTableProcess;
import com.atguigu.edu.realtime.common.EduConfig;
import com.atguigu.edu.realtime.util.DruidDSUtil;
import com.atguigu.edu.realtime.util.PhoenixUtil;
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

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, DimTableProcess> tableConfigDescriptor;

    // 定义 Druid 连接池对象
    DruidDataSource druidDataSource;

    // 定义预加载配置对象
    HashMap<String, DimTableProcess> configMap = new HashMap<>();

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);
        druidDataSource = DruidDSUtil.createDataSource();

        // 预加载配置信息
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/edu_config?" +
                "user=root&password=000000&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        String sql = "select * from edu_config.table_process";
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
            configMap.put(key, jsonValue.toJavaObject(DimTableProcess.class));
        }

        rs.close();
        preparedStatement.close();
        conn.close();
    }


    public MyBroadcastFunction(MapStateDescriptor<String, DimTableProcess> tableConfigDescriptor) {
        this.tableConfigDescriptor = tableConfigDescriptor;
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, DimTableProcess> dimTableProcessState = readOnlyContext.getBroadcastState(tableConfigDescriptor);
        // 获取配置信息
        String sourceTable = jsonObj.getString("table");
        DimTableProcess dimTableProcess = dimTableProcessState.get(sourceTable);

        // 如果状态中没有配置信息，从预加载 Map 中加载一次
        if (dimTableProcess == null) {
            dimTableProcess = configMap.get(sourceTable);
        }

        if (dimTableProcess != null) {
            // 判断操作类型是否为 null，校验数据结构是否完整
            String type = jsonObj.getString("type");
            if (type == null) {
                System.out.println("Maxwell 采集的数据格式异常，缺少操作类型");
            } else {
                JSONObject data = jsonObj.getJSONObject("data");

                String sinkTable = dimTableProcess.getSinkTable();
                String sinkColumns = dimTableProcess.getSinkColumns();

                // 根据 sinkColumns 过滤数据
                filterColumns(data, sinkColumns);

                // 将目标表名加入到主流数据中
                data.put("sink_table", sinkTable);

                // 将操作类型加入到 JSONObject 中
                data.put("type", type);

                collector.collect(data);
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
        BroadcastState<String, DimTableProcess> tableConfigState = context.getBroadcastState(tableConfigDescriptor);
        String op = jsonObj.getString("op");
        if ("d".equals(op)) {
            DimTableProcess before = jsonObj.getObject("before", DimTableProcess.class);
            String sourceTable = before.getSourceTable();
            tableConfigState.remove(sourceTable);
            // 同时删除预加载 Map 中的配置信息
            configMap.remove(sourceTable);
        } else {
            DimTableProcess config = jsonObj.getObject("after", DimTableProcess.class);

            String sourceTable = config.getSourceTable();
            String sinkTable = config.getSinkTable();
            String sinkColumns = config.getSinkColumns();
            String sinkPk = config.getSinkPk();
            String sinkExtend = config.getSinkExtend();

            tableConfigState.put(sourceTable, config);
            // 将更新同步到预加载 Map 中
            configMap.put(sourceTable, config);
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }
    }


    /**
     * Phoenix 建表函数
     *
     * @param sinkTable   目标表名  eg. test
     * @param sinkColumns 目标表字段  eg. id,name,sex
     * @param sinkPk      目标表主键  eg. id
     * @param sinkExtend  目标表建表扩展字段  eg. ""
     *                    eg. create table if not exists mydb.test(id varchar primary key, name varchar, sex varchar)...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 封装建表 SQL
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + EduConfig.HBASE_SCHEMA
                + "." + sinkTable + "(\n");
        String[] columnArr = sinkColumns.split(",");
        // 为主键及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 遍历添加字段信息
        for (int i = 0; i < columnArr.length; i++) {
            sql.append(columnArr[i] + " varchar");
            // 判断当前字段是否为主键
            if (sinkPk.equals(columnArr[i])) {
                sql.append(" primary key");
            }
            // 如果当前字段不是最后一个字段，则追加","
            if (i < columnArr.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        String createStatement = sql.toString();

        DruidPooledConnection conn = null;
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("从 Druid 连接池获取连接对象异常");
        }
        PhoenixUtil.executeDDL(conn, createStatement);
    }

}
