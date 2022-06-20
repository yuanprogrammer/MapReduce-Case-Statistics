import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * FileName:    ddd
 * Author:      Yuan-Programmer
 * Date:        2021/11/23 11:07
 * Description:
 */
public class VideoInfoMap extends Mapper<LongWritable, Text, Text, VideoInfoWritable> {
    @Override
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String line = v1.toString(); //读取清先之后的每一行数据
        String[] fields = line.split("\t"); //通过"\t"对数据进行切割
        String id = fields[0]; //获取主播的id
        /**
         * 获取主播的其他数据
         * gold-------->>金币
         * watchnumpv-->>播放量
         * follower---->>粉丝
         * length------>>开播时长
         */
        long gold = Long.parseLong(fields[1]);
        long watchnumpv = Long.parseLong(fields[2]);
        long follower = Long.parseLong(fields[3]);
        long length = Long.parseLong(fields[4]);
        Text k2 = new Text();
        k2.set(id);
        // 封装到自定义的VideoInfoWritable类对象中
        VideoInfoWritable v2 = new VideoInfoWritable();
        v2.set(gold, watchnumpv, follower, length);
        context.write(k2, v2);
    }
}
