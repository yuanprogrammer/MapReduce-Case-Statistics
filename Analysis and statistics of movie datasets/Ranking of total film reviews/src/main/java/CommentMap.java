import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * FileName:    CommentMap
 * Author:      Yuan-Programmer
 * Date:        2021/11/23 11:07
 * Description:
 */
public class CommentMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    HashMap<String, Integer> map = new HashMap();

    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString();
        String[] fields = line.split("\t");
        String province = fields[0];
        Integer critics = Integer.parseInt(fields[1]);
        map.put(province, critics);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //  根据map中的value进行排序
        Map<String, Integer> sortedMap = MapUtils.sortValue(map);
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
