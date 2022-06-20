import com.itcast.MapUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * FileName:    test
 * Author:      Bald Programmer
 * Date:        2021/11/9 10:00
 * Description:
 */
public class test {
    public static void main(String[] arg0){
        Map map = new HashMap();
        map.put("张三",9);
        map.put("李四",1);
        map.put("老五",84);
        map.put("赵六",15);

        Map returnMap = MapUtils.sortValue(map);
        System.out.println(returnMap);
    }
}
