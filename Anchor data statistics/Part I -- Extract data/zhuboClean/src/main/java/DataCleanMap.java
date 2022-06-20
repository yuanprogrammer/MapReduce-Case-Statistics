import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * FileName:    DataCleanMap
 * Author:      Yuan-Programmer
 * Date:        2021/11/23 9:32
 * Description: 数据清洗类，对大量的数据进行清洗，读取我们所需要的数据
 */
public class DataCleanMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        try {
            String line = v1.toString();//获取每一行内容
            JSONObject jsonObj = new JSONObject(line); //将字符串转换为JSON格式
            String id = jsonObj.getString("uid"); //获取主播的id数据
            /**
             * 获取主播的其他数据
             * gold-------->>金币
             * watchnumpv-->>播放量
             * follower---->>粉丝
             * length------>>开播时长
             */
            int gold = jsonObj.getInt("gold");
            int watchnumpv = jsonObj.getInt("watchnumpv");
            int follower = jsonObj.getInt("follower");
            int length = jsonObj.getInt("length");
            // 过滤掉异常数据（如播放量<0等这些）
            if (gold >= 0 && watchnumpv >= 0 && follower >= 0 && length >= 0) {
                // 封装数据到Text中，最后写入context上下文中
                Text k2 = new Text();
                k2.set(id);
                Text v2 = new Text();
                v2.set(gold + "\t" + watchnumpv + "\t" + follower + "\t" + length);
                context.write(k2, v2);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }
}
