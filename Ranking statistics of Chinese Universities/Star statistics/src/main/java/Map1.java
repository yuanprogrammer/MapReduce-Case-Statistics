import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    Map1
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 8:51
 * Description:
 */
public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString(); //读取清先之后的每一行数据
        String[] fields = line.split("\t"); //通过"\t"对数据进行切割
        /**
         * 获取办学层次
         */
        String name = fields[3];
        Text k2 = new Text();
        k2.set(name);
        IntWritable v2 = new IntWritable();
        v2.set(1);
        // 封装到自定义的VideoInfoWritable类对象中
        context.write(k2, v2);
    }
}
