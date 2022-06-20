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
 * FileName:    UniversityMain
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 9:02
 * Description:
 */
public class UniversityMain {
    public static void main(String[] args) {
        try {
            // 运行jar包程序指令输入错误，直接退出程序
            if (args.length != 2) {
                System.exit(100);
            }

            Configuration conf = new Configuration();//job需要的配置参数
            Job job = Job.getInstance(conf, "UniversityMain");//创建一个job作业
            job.setJarByClass(UniversityMain.class);//设置入口类
            FileInputFormat.setInputPaths(job, new Path(args[0]));//指定输入路径（可以是文件，也可以是目录）
            FileOutputFormat.setOutputPath(job, new Path(args[1]));//指定输出路径（只能是指定一个不存在的目录）
            job.setMapperClass(Map1.class);
            // 指定K2的输出数据类型
            job.setMapOutputKeyClass(Text.class);
            // 指定v2的输出数据类型
            job.setMapOutputValueClass(IntWritable.class);
            // Reduce
//            job.setNumReduceTasks(0);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //提交作业job
            boolean result = false;
            try {
                result = job.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + (result ? "办学层次（university）统计排序完毕！！！" : "统计失败！！！"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
