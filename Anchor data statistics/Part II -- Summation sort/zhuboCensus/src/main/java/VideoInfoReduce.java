import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FileName:    dd
 * Author:      Yuan-Programmer
 * Date:        2021/11/23 11:08
 * Description:
 */
public class VideoInfoReduce extends Reducer<Text,VideoInfoWritable, Text,VideoInfoWritable> {
    @Override
    protected void reduce(Text k2,Iterable<VideoInfoWritable> v2s,Context context) throws IOException,InterruptedException{
        // 从v2s中把相同的k2的value取出来，进行遍历，进行累加求和。
        long goldsum=0;
        long watchnumpvsum=0;
        long followersum=0;
        long lengthsum=0;
        /**
         * v2s：相同主播id的对应对象集合（VideoInfoWritable类对象，有四个属性）
         * 遍历具有相同id的对象，获取对应四个字段值，进行求和
         */
        for(VideoInfoWritable v2:v2s){
            goldsum+=v2.getGold();
            watchnumpvsum+=v2.getWatchnumpv();
            followersum+=v2.getFollower();
            lengthsum+=v2.getLength();
        }
        // 将求和统计好的封装进来，写入context中，交由Job主类打印输出
        Text k3=k2;
        VideoInfoWritable v3=new VideoInfoWritable();
        v3.set(goldsum,watchnumpvsum,followersum,lengthsum);
        context.write(k3,v3);
    }
}
