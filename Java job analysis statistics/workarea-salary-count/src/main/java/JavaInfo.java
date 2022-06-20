import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName:    JavaInfo
 * Author:      Yuan-Programmer
 * Date:        2022/1/2 11:39
 * Description:
 */
public class JavaInfo implements Writable {
    private Integer min_salary; // 最小工资
    private Integer max_salary; // 最大工资
    private Integer count; // 地区出现次数
    private Integer average_salary; // 平均工资

    public Integer getMin_salary() {
        return min_salary;
    }

    public void setMin_salary(Integer min_salary) {
        this.min_salary = min_salary;
    }

    public Integer getMax_salary() {
        return max_salary;
    }

    public void setMax_salary(Integer max_salary) {
        this.max_salary = max_salary;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }


    public Integer getAverage_salary() {
        return average_salary;
    }

    public void setAverage_salary(Integer average_salary) {
        this.average_salary = average_salary;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(min_salary);
        dataOutput.writeInt(max_salary);
        dataOutput.writeInt(count);
        dataOutput.writeInt(average_salary);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.min_salary = dataInput.readInt();
        this.max_salary = dataInput.readInt();
        this.count = dataInput.readInt();
        this.average_salary = dataInput.readInt();
    }

    @Override
    public String toString() {
        return average_salary+"\t"+count+"\t";
    }
}
