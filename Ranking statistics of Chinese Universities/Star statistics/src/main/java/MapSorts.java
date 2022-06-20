
import java.util.Map;
import java.util.*;

/**
 •	Map工具类
 */
public class MapSorts {
    public static Map<String, Integer> sortValue(Map<String,Integer> map) {
        Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(entrySet);

        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {

            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                int result = (o1.getValue() - o2.getValue());
                return -result;
            }
        });

        LinkedHashMap<String, Integer> link = new LinkedHashMap<String, Integer>();

        for (Map.Entry<String, Integer> entry : list) {
            link.put(entry.getKey(), entry.getValue());
        }
        return link;
    }
}
