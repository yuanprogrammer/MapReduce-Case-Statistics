import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * FileName:    CompanyTypeReduce
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 10:43
 * Description:
 */
public class CompanyTypeReduce extends Reducer<Text, IntWritable, Text, Text> {
    /**
     * 执行前执行，只执行一次，打印txt数据第一行的标题
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Text title1 = new Text();
        Text title2 = new Text();
        title1.set("公司规模");
        title2.set("占比");
        context.write(title1, title2);
    }

    public void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
        Integer count = 0;
        for (IntWritable v2 : v2s) {
            count += v2.get();
        }
        System.out.println(k2.toString()+"出现了"+count+"次");
        Text k3 = new Text();
        Text v3 = new Text();
        k3.set(k2);
        v3.set(count.toString());
        context.write(k3, v3);
    }
}
