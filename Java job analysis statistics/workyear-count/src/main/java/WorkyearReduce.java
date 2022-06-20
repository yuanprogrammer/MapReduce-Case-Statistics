import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * FileName:    WorkyearReduce
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 11:19
 * Description:
 */
public class WorkyearReduce extends Reducer<Text, IntWritable, Text, Text> {
    HashMap<String, Integer> map = new HashMap();

    /**
     * 执行前执行，只执行一次，打印txt数据第一行的标题
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Text title1 = new Text();
        Text title2 = new Text();
        title1.set("工作经验");
        title2.set("占比");
        context.write(title1, title2);
    }

    public void reduce(Text k2, Iterable<IntWritable> v2s, Context context) {
        Integer count = 0;
        for (IntWritable v2 : v2s) {
            count += v2.get();
        }
        System.out.println(k2.toString()+"出现了"+count+"次");
        map.put(k2.toString(), count);
    }

    /**
     * 执行后执行，只执行一次，打印最终排序结果
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, Integer> sortedMap = WorkyearSort.sortValue(map); // 对value进行排序
        Set<Map.Entry<String, Integer>> entries = sortedMap.entrySet();
        Iterator<Map.Entry<String, Integer>> it = entries.iterator();
        /**
         * 不设置限制，对所有排序好的数据进行读取
         * 循环获取迭代器的Map对象，再获取对应K,V
         * 将K,V封装到上下文中
         */
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = it.next();
            /* 封装k3，v3 */
            Text k3 = new Text();
            Text v3 = new Text();
            k3.set(entry.getKey());
            v3.set(entry.getValue().toString());
            /* 写入上下文 */
            context.write(k3, v3);
        }
    }
}