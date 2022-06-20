import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * FileName:    CountryMain
 * Author:      Yuan-Programmer
 * Date:        2021/12/26 12:27
 * Description:
 */
public class CountryMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.exit(2);
        }

        Job job = new Job(conf, "CountryCount"); // 新建一个 job，传入配置信息
        job.setJarByClass(CountryMain.class); // 设置 job 的主类
        job.setMapperClass(CountryMap.class); // 设置 job 的 Mapper 类
        job.setCombinerClass(CountryReduce.class); // 设置 job 的 作业合成类
        job.setReducerClass(CountryReduce.class); // 设置 job 的 Reducer 类
        job.setOutputKeyClass(Text.class); // 设置 job 输出数据的关键类
        job.setOutputValueClass(IntWritable.class); // 设置 job 输出值类
        FileInputFormat.addInputPath(job, new Path(args[0])); // 文件输入
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 文件输出
        boolean result = false;
        try {
            result = job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + (result ? "电影类型（Country）统计完毕！！！" : "统计失败！！！"));
    }
}
