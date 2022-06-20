import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    EducationMap
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 10:43
 * Description:
 */
public class EducationMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 对学历要求进行读取切割
     * @param k1
     * @param v1
     * @param context
     */
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        /* 按行读取 */
        String[] line = v1.toString().split("\t");
        // 获取学历数据
        String Education = line[4];
        // 写入到context上下文中，给Reduce进行运算
        Text k2 = new Text();
        IntWritable v2 = new IntWritable();
        k2.set(Education);
        v2.set(1);
        context.write(k2, v2);
    }
}