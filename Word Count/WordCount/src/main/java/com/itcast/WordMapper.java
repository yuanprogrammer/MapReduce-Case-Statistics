package com.itcast;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * FileName:    WordMapper
 * Author:      Yuan-Programmer
 * Date:        2021/11/8 23:29
 * Description:
 */
// 创建一个 WordMapper类 继承于 Mapper抽象类
public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // Mapper抽象类的核心方法，三个参数
    public void map(Object key, // 首字符偏移量
                    Text value, // 文件的一行内容
                    Context context) // Mapper端的上下文，与 OutputCollector 和 Reporter 的功能类似
            throws IOException, InterruptedException {
        String[] ars = value.toString().split("['.;,?| \t\n\r\f]");
        for (String tmp : ars) {
            if (tmp == null || tmp.length() <= 0) {
                continue;
            }
            word.set(tmp);
            System.out.println(new Date().toGMTString() + ":" + word + "出现一次，计数+1");
            context.write(word, one);
        }
    }
}
