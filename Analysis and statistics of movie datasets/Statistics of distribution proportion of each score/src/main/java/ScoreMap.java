import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    ScoreMap
 * Author:      Yuan-Programmer
 * Date:        2021/12/26 12:27
 * Description:
 */
// 创建一个 ScoreMap 继承于 Mapper抽象类
public class ScoreMap extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // Mapper抽象类的核心方法，三个参数
    public void map(Object key, // 首字符偏移量
                    Text value, // 文件的一行内容
                    Context context) // Mapper端的上下文，与 OutputCollector 和 Reporter 的功能类似
            throws IOException, InterruptedException {
        String[] ars = value.toString().split("\t\n");
        for (String tmp : ars) {
            if (tmp == null || tmp.length() <= 0) {
                continue;
            }
            word.set(tmp);
            context.write(word, one);
        }
    }
}