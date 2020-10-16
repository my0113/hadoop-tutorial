package cn.itcast.hadoop.mapreduce.db.imitate.mapper;

import cn.itcast.hadoop.mapreduce.db.imitate.GeneralWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 导出专用Mapper
 * @ClassName ExportMapper
 * @Description
 * @Created by MengYao
 * @Date 2020/10/15 16:17
 * @Version V1.0
 */
public class ExportMapper extends Mapper<LongWritable, Text, NullWritable, GeneralWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
