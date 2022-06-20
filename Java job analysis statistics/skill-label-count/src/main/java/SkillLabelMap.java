import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    SkillLabelMap
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 10:43
 * Description:
 */
public class SkillLabelMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 对学历要求进行读取切割
     * @param k1
     * @param v1
     * @param context
     */
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        /* 按行读取 */
        String[] line = v1.toString().split("\t");
        /* 获取拉勾网的技能标签数，前途无忧没有该部分数据 */
        if (line.length == 8) {
            // 获取学历数据
            String[] skills = line[7].split("[,，|｜]");
            for (String skill : skills) {
                // 写入到context上下文中，给Reduce进行运算
                Text k2 = new Text();
                IntWritable v2 = new IntWritable();
                k2.set(skill.trim());
                v2.set(1);
                context.write(k2, v2);
            }
        }
    }
}