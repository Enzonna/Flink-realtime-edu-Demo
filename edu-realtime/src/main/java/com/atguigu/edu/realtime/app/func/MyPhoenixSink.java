package com.atguigu.edu.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.util.DimUtil;
import com.atguigu.edu.realtime.util.DruidDSUtil;
import com.atguigu.edu.realtime.util.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class MyPhoenixSink extends RichSinkFunction<JSONObject> {

    static DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建连接池
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取目标表名
        String sinkTable = jsonObj.getString("sink_table");
        // 获取操作类型
        String type = jsonObj.getString("type");
        // 获取 id 字段的值
        String id = jsonObj.getString("id");

        // 清除 JSON 对象中的 sinkTable 字段和 type 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sink_table");
        jsonObj.remove("type");

        // 获取连接对象
        DruidPooledConnection conn = null;
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("获取连接对象异常");
        }

        // 执行写入操作
        PhoenixUtil.executeDML(conn, sinkTable, jsonObj);

        // 如果操作类型为 update，则清除 redis 中的缓存信息
        if ("update".equals(type)) {
            DimUtil.deleteCached(sinkTable, id);
        }
    }
}
