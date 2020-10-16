package cn.itcast.hadoop.mapreduce.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName MyFileInputFormat
 * @Description
 * @Created by MengYao
 * @Date 2020/10/12 10:54
 * @Version V1.0
 */
public class MyFileInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        return super.getSplits(job);
    }
}
