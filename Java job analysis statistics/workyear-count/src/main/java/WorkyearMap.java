import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    WorkyearMap
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 11:18
 * Description: 工作经验统计Map阶段
 */
public class WorkyearMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 对工作经验要求进行读取切割
     * @param k1 偏移量
     * @param v1 行数据
     * @param context 上下文
     */
    protected void map(LongWritable k1, Text v1, Mapper.Context context) throws IOException, InterruptedException {
        /* 按行读取 */
        String[] line = v1.toString().split("\t");
        // 获取学历数据
        String Workyear = line[5];
        // 写入到context上下文中，给Reduce进行运算
        Text k2 = new Text();
        IntWritable v2 = new IntWritable();
        k2.set(Workyear);
        v2.set(1);
        context.write(k2, v2);
    }
}
