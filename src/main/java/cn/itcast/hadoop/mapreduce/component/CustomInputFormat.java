package cn.itcast.hadoop.mapreduce.component;

import com.google.common.base.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName SCInputFormat
 * @Description
 * @Created by MengYao
 * @Date 2020/10/21 11:32
 * @Version V1.0
 */
public class CustomInputFormat extends FileInputFormat<LongWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomInputFormat.class);

    public CustomInputFormat() {
        LOG.info("==== 初始化SCInputFormat ====");
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);
        splits.forEach(is -> LOG.info("==== 得到逻辑切块：{} ====", getSplitInfo(is)));
        return splits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        LOG.info("==== 为逻辑切块{}创建LineRecordReader，使用的分隔符为：{} ====", getSplitInfo(split), delimiter);
        LineRecordReader lineRecordReader = new LineRecordReader(recordDelimiterBytes);
        return lineRecordReader;
    }

    /**
     * 从切片中获取文件信息
     * @param split
     * @return
     */
    private String getSplitInfo(InputSplit split) {
        StringBuilder builder = new StringBuilder(32);
        try {
            if (split instanceof FileSplit) {
                FileSplit fis = (FileSplit) split;
                builder.append("File=").append(fis.getPath()).append(",\t")
                        .append("Start=").append(fis.getStart()).append(",\t")
                        .append("Length=").append(fis.getLength()).toString();
            } else {
                SplitLocationInfo[] locationInfo = split.getLocationInfo();
                builder.append("LocationInfos=[");
                for (SplitLocationInfo splitLocationInfo : locationInfo) {
                    builder.append("LocationInfo=").append(splitLocationInfo.getLocation()).append(",InMemory=").append(splitLocationInfo.isInMemory()).append(",");
                }
                builder.append("]\t");
                String[] locations = split.getLocations();
                builder.append("locations=[");
                for (String location : locations) {
                    builder.append("location=").append(location).append(",");
                }
                builder.append("]\t");
                long length = split.getLength();
                builder.append("Length="+length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return builder.toString();
    }

}
