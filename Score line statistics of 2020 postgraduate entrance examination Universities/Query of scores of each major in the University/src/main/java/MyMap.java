import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * FileName:    MyMap
 * Author:      小袁教程
 * Date:        2022/6/15 18:32
 * Description:
 */
public class MyMap extends Mapper<LongWritable, Text, Text, NullWritable> {

    private String searchWord;

    private Map<String, String> map = new HashMap<String, String>();

    private static Map<String, University> resultMap = new HashMap<String, University>();

    private static int count = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取筛选条件
        Configuration configuration = context.getConfiguration();
        this.searchWord = configuration.get("searchWord");

        // 设置标题
        context.write(new Text("学校名称\t学院名称\t专业名称\t分数线（高到低）"), NullWritable.get());
    }

    @Override
    public void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        count++;

        //读取清先之后的每一行数据
        String line = v1.toString();

        //通过","对数据进行切割
        String[] fields = line.split(",");

        try {
            // 大学
            String university = fields[2];
            // 学院
            String institute = fields[4];
            // 专业
            String specialized = fields[7];
            // 分数线
            int totalScore = Integer.parseInt(fields[8]);

            if (this.searchWord.equals(university) && this.map.get(specialized) == null) {
                // 标识这个大学的这个专业已经出现了, 防止重复数据
                this.map.put(specialized, university);

                resultMap.put(specialized, new University(university, institute, specialized, totalScore));
            }
        }catch (NumberFormatException e) {
//            System.out.println("数据不合法，跳过数据");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (count == 60000) {
            // 转换
            List<Map.Entry<String, University>> list = new ArrayList<Map.Entry<String, University>>(resultMap.entrySet());

            // 对平均分数线排序
            Collections.sort(list, new Comparator<Map.Entry<String, University>>() {
                public int compare(Map.Entry<String, University> o1, Map.Entry<String, University> o2) {
                    return o2.getValue().getScore() - o1.getValue().getScore();
                }
            });

            // 遍历排序后的数据, 写入最后结果中
            for (Map.Entry<String, University> entry : list) {
                System.out.println(entry.getValue().toString());

                // 写入最终结果
                context.write(new Text(entry.getValue().toString()), NullWritable.get());
            }
        }
    }

}