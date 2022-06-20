import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
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
public class Reduce extends Reducer<Text, UniversityInfo, Text, Text> {

    Map<String, UniversityInfo> map = new HashMap<String, UniversityInfo>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.out.println("办学层次"+"\t"+"平均得分排行"+"\t"+"平均星级");
        Text t1 = new Text();
        t1.set("办学层次");
        Text t2 = new Text();
        t2.set("平均得分排行"+"\t"+"平均星级");
        context.write(t1, t2);
    }

    @Override
    protected void reduce(Text k2, Iterable<UniversityInfo> v2s, Context context) throws IOException, InterruptedException {
        // 从v2s中把相同的k2的value取出来，进行遍历，进行累加求和。
        Integer num = 0; //统计长度
        double sum = 0;
        Integer sum1 = 0;
        BigDecimal d1 = new BigDecimal(sum);
        for (UniversityInfo v2 : v2s) {
            num += 1;
            sum1 += v2.getStart();
            BigDecimal d2 = new BigDecimal(v2.getScore());
            d1=d1.add(d2);
        }
        BigDecimal d2 = new BigDecimal(num);
        d1 = d1.divide(d2, 2, BigDecimal.ROUND_HALF_UP);
        UniversityInfo info = new UniversityInfo();
        info.set(d1.doubleValue(), sum1/num);
        map.put(k2.toString(), info);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //  根据map中的value进行排序
        Map<String, UniversityInfo> sortedMap = MapSorts.sortValue(map);
        Set<Map.Entry<String, UniversityInfo>> entries = sortedMap.entrySet();
        Iterator<Map.Entry<String, UniversityInfo>> it = entries.iterator();

        while (it.hasNext()) {
            //  获取Map
            Map.Entry<String, UniversityInfo> entry = it.next();
            String key = entry.getKey();
            UniversityInfo info = entry.getValue();
            //  封装k3,v3
            Text k3 = new Text();
            k3.set(key);
            Text v3 = new Text();
            v3.set(info.toString());
            //  写入上下文
            System.out.println(key+"\t"+info.toString());
            context.write(k3, v3);
        }
    }
}
