import java.util.*;

/**
 * FileName:    WorkyearSort
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 11:19
 * Description:
 */
public class WorkareaSort {
    public static Map<String, JavaInfo> sortValue(Map<String,JavaInfo> map) {
        Set<Map.Entry<String, JavaInfo>> entrySet = map.entrySet();
        List<Map.Entry<String, JavaInfo>> list = new ArrayList<Map.Entry<String, JavaInfo>>(entrySet);

        /**
         * 对JavaInfo中的average平均工资进行降序排序
         */
        Collections.sort(list, new Comparator<Map.Entry<String, JavaInfo>>() {

            @Override
            public int compare(Map.Entry<String, JavaInfo> o1, Map.Entry<String, JavaInfo> o2) {
                int result = (o1.getValue().getAverage_salary() - o2.getValue().getAverage_salary());
                return -result;
            }
        });

        LinkedHashMap<String, JavaInfo> sortResult = new LinkedHashMap<String, JavaInfo>();

        for (Map.Entry<String, JavaInfo> entry : list) {
            sortResult.put(entry.getKey(), entry.getValue());
        }
        return sortResult;
    }
}
