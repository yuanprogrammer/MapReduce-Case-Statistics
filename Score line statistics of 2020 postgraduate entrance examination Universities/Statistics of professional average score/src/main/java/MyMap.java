import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    MyMap
 * Author:      小袁教程
 * Date:        2022/6/15 18:32
 * Description:
 */
public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //读取清先之后的每一行数据
        String line = v1.toString();

        //通过","对数据进行切割
        String[] fields = line.split(",");

        try {
            String university = fields[7];
            int totalScore = Integer.parseInt(fields[8]);

            Text k2 = new Text();
            k2.set(university);

            IntWritable v2 = new IntWritable();
            v2.set(totalScore);

            context.write(k2, v2);
        }catch (NumberFormatException e) {
            System.out.println("数据不合法，跳过数据");
        }

    }
}