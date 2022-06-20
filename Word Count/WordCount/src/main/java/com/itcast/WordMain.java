package com.itcast;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * FileName:    WordMain
 * Author:      Yuan-Programmer
 * Date:        2021/11/8 23:33
 * Description:
 */
public class WordMain {

    public static void main(String[] args) throws Exception {
        // Configuration类：读取Hadoop的配置文件，如 site-core.xml...；
        // 也可用set方法重新设置（会覆盖）：conf.set("fs.default.name", "hdfs://xxxx:9000")
        Configuration conf = new Configuration();
        // 将命令行中参数自动设置到变量conf中
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        /**
         * 这里必须有输入输出
         */

        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        /**
         * 获取当前时间，存放到上下文
         */
//        String dt = DateFormatUtils.format(new Date(), "yyyy-MM-dd hh:mm:ss");
//        conf.set("dt",dt);

        Job job = new Job(conf, "word count"); // 新建一个 job，传入配置信息
        job.setJarByClass(WordMain.class); // 设置 job 的主类
        job.setMapperClass(WordMapper.class); // 设置 job 的 Mapper 类
        job.setCombinerClass(WordReducer.class); // 设置 job 的 作业合成类
        job.setReducerClass(WordReducer.class); // 设置 job 的 Reducer 类
        job.setOutputKeyClass(Text.class); // 设置 job 输出数据的关键类
        job.setOutputValueClass(IntWritable.class); // 设置 job 输出值类
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 文件输入
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); // 文件输出
        boolean result = false;
        try {
            result = job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(new Date().toGMTString() + (result ? "成功" : "失败"));
        System.exit(result ? 0 : 1); // 等待完成退出
    }
}
