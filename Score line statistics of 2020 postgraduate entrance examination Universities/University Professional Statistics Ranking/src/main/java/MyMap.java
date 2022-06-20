import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * FileName:    MyMap
 * Author:      小袁教程
 * Date:        2022/6/15 18:32
 * Description:
 */
public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static HashMap<String, String> map = new HashMap<String, String>();

    @Override
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //读取清先之后的每一行数据
        String line = v1.toString();

        //通过","对数据进行切割
        String[] fields = line.split(",");

        String university = fields[2];
        String institute = fields[4];
        String specialty = fields[7];
        String score = fields[8];

        if (!("学校名称".equals(university)) && !("-".equals(score))) {
            if (map.get(university + "-" + institute + "-" + specialty) == null) {
                map.put(university + "-" + institute + "-" + specialty, "1");

                Text k2 = new Text();
                k2.set(university);

                IntWritable v2 = new IntWritable();
                v2.set(1);

                context.write(k2, v2);
            }
        }
    }
}