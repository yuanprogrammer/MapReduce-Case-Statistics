import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    CompanyTypeMap
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 10:43
 * Description:
 */
public class CompanyTypeMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 对公司规模类型进行读取切割
     * @param k1
     * @param v1
     * @param context
     */
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        /* 按行读取 */
        String[] line = v1.toString().split("\t");
        /* 筛选，只获取前途无忧的数据，拉勾网数据中没有公司规模数据 */
        if (line.length == 9) {
            // 获取公司类型规模数据
            String CompanyType = line[7];
            // 写入到context上下文中，给Reduce进行运算
            Text k2 = new Text();
            IntWritable v2 = new IntWritable();
            k2.set(CompanyType);
            v2.set(1);
            context.write(k2, v2);
        }
    }
}