import org.junit.Test;

import java.math.BigDecimal;

/**
 * FileName:    dwd
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 9:27
 * Description:
 */
public class dwd {

    @Test
    public void test() {
        double s1 = 19.99;
        Integer s2 = 20;
        BigDecimal d1 = new BigDecimal(s1);
        BigDecimal d2 = new BigDecimal(s2);
        d1 = d1.multiply(d2);
        System.out.println(d1);
    }
}
