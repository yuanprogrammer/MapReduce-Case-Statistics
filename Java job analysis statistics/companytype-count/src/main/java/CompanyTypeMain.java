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
 * FileName:    CompanyTypeMain
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 10:43
 * Description:
 */
public class CompanyTypeMain {
    public static void main(String[] args) {
        try {
            // 运行jar包程序指令输入错误，直接退出程序
            if (args.length != 2) {
                System.exit(100);
            }
            Configuration conf = new Configuration();//job需要的配置参数
            Job job = Job.getInstance(conf, "CompanyTypeMain");//创建一个job作业
            job.setJarByClass(CompanyTypeMain.class);//设置入口类
            FileInputFormat.setInputPaths(job, new Path(args[0]));//指定输入路径（可以是文件，也可以是目录）
            FileOutputFormat.setOutputPath(job, new Path(args[1]));//指定输出路径（只能是指定一个不存在的目录）
            /* Map阶段相关设置 */
            job.setMapperClass(CompanyTypeMap.class);
            job.setMapOutputKeyClass(Text.class); // k2
            job.setMapOutputValueClass(IntWritable.class); // v2
            /* Reduce阶段相关设置 */
            job.setReducerClass(CompanyTypeReduce.class);
            job.setOutputKeyClass(Text.class); // k3
            job.setOutputValueClass(Text.class); // v3
            //提交作业job
            boolean result = false;
            try {
                result = job.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("\t"+new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()) + (result ? "公司规模类型（Java-Job）统计完毕！！！" : "统计失败！！！"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
