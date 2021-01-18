package com.atguigu.day01;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class Fink01_WordCount_Batch {
    public static void main(String[] args) {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件数据
        DataSource<String> lineDS = env.readTextFile("input");
        //3.
    }
}
