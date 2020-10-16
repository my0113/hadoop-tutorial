package cn.itcast.hadoop.mapreduce.db.imitate.mapper;

import cn.itcast.hadoop.mapreduce.db.imitate.GeneralWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 导入专用Mapper
 * @ClassName ImportMapper
 * @Description
 * @Created by MengYao
 * @Date 2020/10/15 16:17
 * @Version V1.0
 */
public class ImportMapper extends Mapper<LongWritable, GeneralWritable, NullWritable, Text> {
    private NullWritable outputKey;
    private Text outputValue;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.outputKey = NullWritable.get();
        this.outputValue = new Text();
    }
    @Override
    protected void map(LongWritable key, GeneralWritable value, Context context) throws IOException, InterruptedException {
        outputValue.set(value.toString());
        context.write(outputKey, outputValue);
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.outputKey = null;
        this.outputValue = null;
    }
}
