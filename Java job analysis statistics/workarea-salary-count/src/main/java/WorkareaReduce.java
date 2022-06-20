import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * FileName:    WorkyearReduce
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 11:19
 * Description:
 */
public class WorkareaReduce extends Reducer<Text, JavaInfo, Text, Text> {
    HashMap<String, JavaInfo> map = new HashMap();

    /**
     * 执行前执行，只执行一次，打印txt数据第一行的标题
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Text title1 = new Text();
        Text title2 = new Text();
        title1.set("工作地区");
        title2.set("平均工资"+"\t"+"占比");
        context.write(title1, title2);
    }

    public void reduce(Text k2, Iterable<JavaInfo> v2s, Context context) {
        JavaInfo javaInfo = new JavaInfo();
        Integer count = 0;          // 出现次数
        Integer min_salary_sum = 0; // 最低工资总和
        Integer max_salary_sum = 0; // 最高工资总和
        Integer average_salary = 0; // 平均工资
        for (JavaInfo v2 : v2s) {
            count++;
            min_salary_sum += v2.getMin_salary();
            max_salary_sum += v2.getMax_salary();
        }
        average_salary = (min_salary_sum + max_salary_sum) / 2 / count;
        javaInfo.setCount(count);
        javaInfo.setAverage_salary(average_salary);
        map.put(k2.toString(), javaInfo);
    }

    /**
     * 执行后执行，只执行一次，打印最终排序结果
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, JavaInfo> sortedMap = WorkareaSort.sortValue(map);// 对value进行排序
        Set<Map.Entry<String, JavaInfo>> entries = sortedMap.entrySet();
        Iterator<Map.Entry<String, JavaInfo>> it = entries.iterator();
        /**
         * 不设置限制，对所有排序好的数据进行读取
         * 循环获取迭代器的Map对象，再获取对应K,V
         * 将K,V封装到上下文中
         */
        while (it.hasNext()) {
            Map.Entry<String, JavaInfo> entry = it.next();
            /* 封装k3，v3 */
            Text k3 = new Text();
            Text v3 = new Text();
            k3.set(entry.getKey());
            v3.set(entry.getValue().toString());
            /* 写入上下文 */
            System.out.println(k3.toString()+"\t"+v3.toString());
            context.write(k3, v3);
        }
    }
}