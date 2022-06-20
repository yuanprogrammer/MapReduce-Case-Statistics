import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * FileName:    Map1
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 8:51
 * Description:
 */
public class Map1 extends Mapper<LongWritable, Text, Text, UniversityInfo> {

    @Override
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString(); //读取清先之后的每一行数据
        String[] fields = line.split("\t"); //通过"\t"对数据进行切割
        UniversityInfo v2 = new UniversityInfo();
        String name = fields[4];
        double score = Double.parseDouble(fields[2]);
        Integer start = Integer.parseInt(fields[3]);
        Text k2 = new Text();
        k2.set(name);
        v2.set(score, start);
        // 封装到自定义的VideoInfoWritable类对象中
        context.write(k2, v2);
    }
}
