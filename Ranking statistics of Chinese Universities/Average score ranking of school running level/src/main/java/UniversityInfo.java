import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName:    UniversityInfo
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 12:22
 * Description:
 */
public class UniversityInfo implements Writable {
    private Double score;
    private Integer start;

    public void set(Double score, Integer start) {
        this.score = score;
        this.start = start;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Integer getStart() {
        return start;
    }

    public void setStart(Integer start) {
        this.start = start;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.score = dataInput.readDouble();
        this.start = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(score);
        dataOutput.writeInt(start);
    }

    @Override
    public String toString() {
        return score + "\t" + start;
    }
}
