/**
 * FileName:    MapUtils
 * Author:      Yuan-Programmer
 * Date:        2021/11/9 0:05
 * Description:
 */

import java.util.*;

/**
 •	Map工具类
 */
public class MapUtils {
    /**
     –	根据Map的value值降序排序
     –	@param map
     –	@param
     –	@param
     –	@return
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                /**
                 * o1-o2：升序
                 * o2-o1：降序
                 * 返回结果加上“-”表示取反操作（升序->降序，降序->升序）
                 */
                int compare = (o1.getValue()).compareTo(o2.getValue());
                return -compare;
            }
        });

        //将排序结果返回上一级
        Map<K, V> returnMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            returnMap.put(entry.getKey(), entry.getValue());
        }
        return returnMap;
    }
}
