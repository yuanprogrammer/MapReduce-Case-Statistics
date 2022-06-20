import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * FileName:    Reduce
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 9:08
 * Description:
 */
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    Map<String, Integer> map = new HashMap<String, Integer>();

    @Override
    protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
        // 从v2s中把相同的k2的value取出来，进行遍历，进行累加求和。
        Integer sum = 0;
        for (IntWritable v2 : v2s) {
            sum+=v2.get();
        }
        map.put(k2.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //  根据map中的value进行排序
        Map<String, Integer> sortedMap = MapSorts.sortValue(map);
        Set<Map.Entry<String, Integer>> entries = sortedMap.entrySet();
        Iterator<Map.Entry<String, Integer>> it = entries.iterator();
        /**
         * 不设置count，对全部单词进行排序
         * 循环获取迭代器的Map对象，再获取对应K,V
         * 将K,V封装到上下文中
         */
        while (it.hasNext()) {
            //  获取Map
            Map.Entry<String, Integer> entry = it.next();
            String key = entry.getKey();
            Integer value = entry.getValue();
            //  封装k3,v3
            Text k3 = new Text();
            k3.set(key);
            IntWritable v3 = new IntWritable();
            v3.set(value);
            //  写入上下文
            context.write(k3, v3);
        }
    }
}
