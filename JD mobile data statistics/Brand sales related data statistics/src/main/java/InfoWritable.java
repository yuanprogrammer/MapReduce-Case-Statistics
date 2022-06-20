import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName:    InfoWritable
 * Author:      Yuan-Programmer
 * Date:        2021/12/29 8:52
 * Description:
 */
public class InfoWritable implements Writable {
//    private String brand;
    private Double price;
    private Integer sales;
    private Integer good;

    public void set(Double price, Integer sales, Integer good) {
//        this.brand = brand;
        this.price = price;
        this.sales = sales;
        this.good = good;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getSales() {
        return sales;
    }

    public void setSales(Integer sales) {
        this.sales = sales;
    }

    public Integer getGood() {
        return good;
    }

    public void setGood(Integer good) {
        this.good = good;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
//        this.brand = dataInput.readLine();
        this.price = dataInput.readDouble();
        this.sales = dataInput.readInt();
        this.good = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeChars(brand);
        dataOutput.writeDouble(price);
        dataOutput.writeInt(sales);
        dataOutput.writeInt(good);
    }

    @Override
    public String toString() {
        return price + "\t" + sales + "\t" + good;
    }
}
