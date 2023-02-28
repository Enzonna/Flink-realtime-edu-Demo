package com.atguigu.edu.realtime.bean;

import lombok.Data;

@Data
public class DwdTableProcess {

    // 来源表
    String sourceTable;

    // 操作类型
    String sourceType;

    // 输出表
    String sinkTable;

    // 输出字段
    String sinkColumns;
}
