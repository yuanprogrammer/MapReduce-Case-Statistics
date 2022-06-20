import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    WorkyearMap
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 11:18
 * Description:
 */
public class WorkareaMap extends Mapper<LongWritable, Text, Text, JavaInfo> {
    /**
     * 对工作经验要求进行读取切割
     * @param k1 偏移量
     * @param v1 行数据
     * @param context 上下文
     */
    protected void map(LongWritable k1, Text v1, Mapper.Context context) throws IOException, InterruptedException {
        /* 按行读取 */
        String[] line = v1.toString().split("\t");
        // 设置k2, v2
        Text k2 = new Text();
        JavaInfo v2 = new JavaInfo();
        // 获取最小工资、最大工资、地区
        String area = line[6];
        v2.setMin_salary(Integer.parseInt(line[2]));
        v2.setMax_salary(Integer.parseInt(line[3]));
        v2.setCount(1);
        v2.setAverage_salary(0);
        // 写入到context上下文中，给Reduce进行运算
        k2.set(area);
        context.write(k2, v2);
    }
}
