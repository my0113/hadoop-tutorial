package cn.itcast.hadoop.mapreduce.compress.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * 将输入的普通文件作为SequenceFile输出
 * @ClassName SequenceFileApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/12 10:02
 * @Version V1.0
 */
public class WriteSeqFileApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = WriteSeqFileApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(WriteSeqFileApp.class);

    /**
     * 实现Mapper类
     */
    static class WriteSeqFileAppMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private NullWritable outputKey;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(outputKey, value);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
        }
    }

    /**
     * 实现Reduce类
     */
    static class WriteSeqFileAppReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private NullWritable outputKey;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
        }
        @Override
        protected void reduce(NullWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iterator = value.iterator();
            while (iterator.hasNext()) {
                context.write(outputKey, iterator.next());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
        }
    }

    /**
     * 重写Tool接口的run方法，用于提交作业
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(this.getClass());
        // 设置作业的输入为TextInputFormat（普通文本）
        job.setInputFormatClass(TextInputFormat.class);
        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置Map端的实现类
        job.setMapperClass(WriteSeqFileAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置作业的输出为SequenceFileOutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 使用SequenceFile的块级别压缩
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        // 设置Reduce端的实现类
        job.setReducerClass(WriteSeqFileAppReducer.class);
        // 设置Reduce端输出的Key类型
        job.setOutputKeyClass(NullWritable.class);
        // 设置Reduce端输出的Value类型
        job.setOutputValueClass(Text.class);
        // 从参数中获取输出路径
        Path outputDir = new Path(args[1]);
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputDir);
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 创建作业
     * @param args
     * @return
     */
    public static int createJob(String[] args) {
        Configuration conf = new Configuration();
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.set(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, "7200000");
        int status = 0;
        try {
            status = ToolRunner.run(conf, new WriteSeqFileApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    /**
     * 主程序入口
     * @param args
     */
    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data1/", "/apps/mapreduce/seq_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
