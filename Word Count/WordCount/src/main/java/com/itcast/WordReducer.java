package com.itcast;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * FileName:    WordReduce
 * Author:      Yuan-Programmer
 * Date:        2021/11/8 23:32
 * Description:
 */
// 创建一个 WordReducer类 继承于 Reducer抽象类
public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable(); // 用于记录 key 的最终的词频数
    HashMap<String, Integer> map = new HashMap();

    // Reducer抽象类的核心方法，三个参数
    public void reduce(Text key, // Map端 输出的 key 值
                       Iterable<IntWritable> values, // Map端 输出的 Value 集合（相同key的集合）
                       Context context) // Reduce 端的上下文，与 OutputCollector 和 Reporter 的功能类似
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) // 遍历 values集合，并把值相加
        {
            sum += val.get();
        }
        map.put(key.toString(), sum);
        System.out.println(new Date().toGMTString() + ":" + key + "出现了" + result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //  根据map中的value进行排序
        Map<String, Integer> sortedMap = MapUtils.sortValue(map);
        Set<Map.Entry<String, Integer>> entries = sortedMap.entrySet();
        Iterator<Map.Entry<String, Integer>> it = entries.iterator();
        /**
         * 不设置count，对全部单词进行排序
         * 循环获取迭代器的Map对象，再获取对应K,V
         * 将K,V封装到上下文中
         */
        while (it.hasNext()) {
            //  获取Map
            Map.Entry<String, Integer> entry = it.next();
            String key = entry.getKey();
            Integer value = entry.getValue();
            //  封装k3,v3
            Text k3 = new Text();
            k3.set(key);
            IntWritable v3 = new IntWritable();
            v3.set(value);
            //  写入上下文
            context.write(k3, v3);
        }
    }
}
