import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * FileName:    dd
 * Author:      Yuan-Programmer
 * Date:        2021/11/23 10:16
 * Description: 入口类（也叫主类）
 */
public class DataCleanJob {
    public static void main(String[] args) {
        try {
            // 运行程序指令输入错误，直接退出程序
            if (args.length != 2) {
                System.exit(100);
            }
            Configuration conf = new Configuration(); //job需要的配置参数
            Job job = Job.getInstance(conf); //创建一个job
            job.setJarByClass(DataCleanJob.class);
            FileInputFormat.setInputPaths(job, new Path(args[0]));//指定输入路径（可以是文件，也可以是目录）
            FileOutputFormat.setOutputPath(job, new Path(args[1])); //指定输出路径（只能是指定一个不存在的目录）
            //指定map相关代码
            job.setMapperClass(DataCleanMap.class);
            //指定K2的类型
            job.setMapOutputKeyClass(Text.class);
            //指定v2的类型
            job.setMapOutputValueClass(Text.class);
            //设置reduce的数量，0表示禁用reduce
            job.setNumReduceTasks(0);
            //提交作业job
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}