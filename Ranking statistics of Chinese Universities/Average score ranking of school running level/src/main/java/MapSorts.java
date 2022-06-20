
import java.util.Map;
import java.util.*;

/**
 •	Map工具类
 */
public class MapSorts {
    public static Map<String, UniversityInfo> sortValue(Map<String,UniversityInfo> map) {
        Set<Map.Entry<String, UniversityInfo>> entrySet = map.entrySet();
        List<Map.Entry<String, UniversityInfo>> list = new ArrayList<Map.Entry<String, UniversityInfo>>(entrySet);

        Collections.sort(list, new Comparator<Map.Entry<String, UniversityInfo>>() {

            @Override
            public int compare(Map.Entry<String, UniversityInfo> o1, Map.Entry<String, UniversityInfo> o2) {
                int result = (int) (o1.getValue().getScore() - o2.getValue().getScore());
                return -result;
            }
        });

        LinkedHashMap<String, UniversityInfo> link = new LinkedHashMap<String, UniversityInfo>();

        for (Map.Entry<String, UniversityInfo> entry : list) {
            link.put(entry.getKey(), entry.getValue());
        }
        return link;
    }
}
