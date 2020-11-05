package cn.itcast.hadoop.mapreduce.compress.snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 将文本文件处理为snappy
 * @ClassName WriteSnappyApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/30 17:58
 * @Version V1.0
 */
public class WriteSnappyApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = WriteSnappyApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(WriteSnappyApp.class);


    /**
     * 实现Mapper类
     */
    public static class WriteSnappyAppMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private NullWritable outputKey;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(outputKey, value);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
        }
    }

    /**
     * 构建并提交作业
     * @param args
     * @return
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(WriteSnappyApp.class);
        // 设置Map端的实现类
        job.setMapperClass(WriteSnappyAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置输入文件路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置作为的Reducer数量为0
        job.setNumReduceTasks(0);
        // 从参数中获取输出路径
        Path outputDir = new Path(args[1]);
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputDir);
        // 设置启用输出压缩
        FileOutputFormat.setCompressOutput(job, true);
        // 设置输出压缩格式为Snappy（如果是Apache版Hadoop则必须自行编译才支持Snappy）
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
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
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
        // 如果core-site.xml没有配置的话，直接在代码中配置即可
        conf.set("io.compression.codecs",
                "org.apache.hadoop.io.compress.GzipCodec," +
                "org.apache.hadoop.io.compress.DefaultCodec," +
                "org.apache.hadoop.io.compress.BZip2Codec," +
                "org.apache.hadoop.io.compress.SnappyCodec");
        int status = 0;
        try {
            status = ToolRunner.run(conf, new WriteSnappyApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data1/", "/apps/mapreduce/snappy_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
