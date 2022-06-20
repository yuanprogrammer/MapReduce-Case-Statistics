import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * FileName:    MyReduce
 * Author:      小袁教程
 * Date:        2022/6/15 18:32
 * Description:
 */
public class MyReduce extends Reducer<Text, IntWritable, Text, Text> {

    private Map<String, Integer> map = new HashMap<String, Integer>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 设置标题
        Text t1 = new Text("学校名称");
        Text t2 = new Text("专业数量");

        System.out.println("学校名称" + "\t" + "专业数量");

        context.write(t1, t2);
    }

    @Override
    protected void reduce(Text k2,Iterable<IntWritable> v2s,Context context) throws IOException,InterruptedException{
        /**
         * 输入：k2 -> 学校名称, v2s -> 同一所学校的所有专业
         * 输出：k3 -> 也就是k2学校名称, v3 -> 学校专业数量
         *
         * total：学校的专业数量
         */

        int total = 0;

        for (IntWritable v2 : v2s) {
            total += v2.get();
        }

        map.put(k2.toString(), total);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 转换
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());

        // 对学校专业数量排序
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });

        // 遍历排序后的数据
        for (Map.Entry<String, Integer> entry : list) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());

            // k3 学校名称
            Text k3 = new Text();
            k3.set(entry.getKey());

            // v3 学校专业数量
            Text v3 = new Text();
            v3.set(Integer.toString(entry.getValue()));

            // 最后结果, 写入上下文
            context.write(k3, v3);
        }
    }
}
