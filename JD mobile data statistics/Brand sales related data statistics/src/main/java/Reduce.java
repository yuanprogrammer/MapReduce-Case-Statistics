import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * FileName:    Reduce
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 9:08
 * Description:
 */
public class Reduce extends Reducer<Text, InfoWritable, Text, Text> {
    @Override
    protected void reduce(Text k2, Iterable<InfoWritable> v2s, Context context) throws IOException, InterruptedException {
        // 从v2s中把相同的k2的value取出来，进行遍历，进行累加求和。
        long total = 0;
        Integer goods = 0;
        Integer sales = 0;
        Integer sum = 0;
        for (InfoWritable v2 : v2s) {
            sum += 1;
            BigDecimal d1 = new BigDecimal(v2.getPrice());
            BigDecimal d2 = new BigDecimal(v2.getSales());
            total += d1.multiply(d2).longValue();
            sales += v2.getSales();
            goods += v2.getGood();
        }
        goods = goods / sum;
        // 将求和统计好的封装进来，写入context中，交由Job主类打印输出
        Text k3 = k2;
        Text v3 = new Text();
        v3.set(sales + "\t" + total + "\t" + goods);
        context.write(k3, v3);
    }
}
