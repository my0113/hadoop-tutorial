package cn.itcast.hadoop.mapreduce.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @ClassName AggregateBean
 * @Description
 * @Created by MengYao
 * @Date 2020/10/22 17:01
 * @Version V1.0
 */
public class AggregateBean implements WritableComparable<AggregateBean> {

    private double max;
    private double min;
    private double avg;
    private double sum;
    private long count;


    @Override
    public int compareTo(AggregateBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(max);
        out.writeDouble(min);
        out.writeDouble(avg);
        out.writeDouble(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.max = in.readDouble();
        this.min = in.readDouble();
        this.avg = in.readDouble();
        this.sum = in.readDouble();
        this.count = in.readLong();
    }

    public void add(double value) {
        setMax(value);
        setMin(value);
        setSum(value);
        setCount(1);
    }

    public void merge(AggregateBean bean) {
        setMax(bean.getMax());
        setMin(bean.getMin());
        setSum(bean.getSum());
        setCount(bean.getCount());
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        if (this.max < max) {
            this.max = max;
        }
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        if (count==0) {
            this.min = min;
        } else {
            if (this.min > min) {
                this.min = min;
            }
        }
    }

    public double getAvg() {
        return avg=(sum/count);
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum += sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count += count;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("max=").append(max).append(", ")
                .append("min=").append(min).append(", ")
                .append("avg=").append(getAvg()).append(", ")
                .append("sum=").append(sum).append(", ")
                .append("count=").append(count)
                .toString();
    }

}
