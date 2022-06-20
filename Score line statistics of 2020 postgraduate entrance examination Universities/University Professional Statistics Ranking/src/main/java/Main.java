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
 * FileName:    Main
 * Author:      小袁教程
 * Date:        2022/6/15 18:31
 * Description:
 */
public class Main {
    public static void main(String[] args) {
        try {
            // 运行jar包程序指令输入错误，直接退出程序
            if (args.length != 2) {
                System.exit(100);
            }
            Configuration conf = new Configuration();//job需要的配置参数

            Job job = Job.getInstance(conf);//创建一个job作业

            job.setJarByClass(Main.class);//设置入口类
            FileInputFormat.setInputPaths(job, new Path(args[0]));//指定输入路径（可以是文件，也可以是目录）
            FileOutputFormat.setOutputPath(job, new Path(args[1]));//指定输出路径（只能是指定一个不存在的目录）

            // 指定Mapper阶段的相关类
            job.setMapperClass(MyMap.class);
            // 指定K2的输出数据类型
            job.setMapOutputKeyClass(Text.class);
            // 指定v2的输出数据类型
            job.setMapOutputValueClass(IntWritable.class);

            // 指定Reduce阶段的相关类
            job.setReducerClass(MyReduce.class);
            // 指定K3的输出数据类型
            job.setOutputKeyClass(Text.class);
            // 指定V3的输出数据类型
            job.setOutputValueClass(Text.class);

            //提交作业job
            boolean result = job.waitForCompletion(true);

            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) +
                    "\t" + "2020年考研各学校开放专业数量统计" + (result ? "成功！" : "失败！"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
