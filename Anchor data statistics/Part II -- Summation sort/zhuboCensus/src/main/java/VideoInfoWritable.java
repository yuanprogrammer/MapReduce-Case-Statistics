import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * FileName:    ddd
 * Author:      Yuan-Programmer
 * Date:        2021/11/23 11:06
 * Description:
 */
public class VideoInfoWritable implements Writable {
    private long gold;
    private long watchnumpv;
    private long follower;
    private long length;

    public void set(long gold, long watchnumpv, long follower, long length) {
        this.gold = gold;
        this.watchnumpv = watchnumpv;
        this.follower = follower;
        this.length = length;
    }

    public long getGold() {
        return gold;
    }

    public long getWatchnumpv() {
        return watchnumpv;
    }

    public long getFollower() {
        return follower;
    }

    public long getLength() {
        return length;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.gold = dataInput.readLong();
        this.watchnumpv = dataInput.readLong();
        this.follower = dataInput.readLong();
        this.length = dataInput.readLong();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(gold);
        dataOutput.writeLong(watchnumpv);
        dataOutput.writeLong(follower);
        dataOutput.writeLong(length);
    }

    @Override
    public String toString() {
        return gold + "\t" + watchnumpv + "\t" + follower + "\t" + length;
    }
}
