import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    Map
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 8:51
 * Description:
 */
public class Map extends Mapper<LongWritable, Text, Text, InfoWritable> {
    @Override
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString(); //读取清先之后的每一行数据
        String[] fields = line.split(","); //通过"\t"对数据进行切割
        /**
         * 获取手机数据
         */
        String brand = fields[0];
        Double price = Double.parseDouble(fields[2]);
        Integer sales = Integer.parseInt(fields[3]);
        Integer good = Integer.parseInt(fields[4]);
        Text k2 = new Text();
        k2.set(brand);
        // 封装到自定义的VideoInfoWritable类对象中
        InfoWritable v2 = new InfoWritable();
        v2.set(price, sales, good);
        context.write(k2, v2);
    }
}
