package cn.itcast.hadoop.mapreduce.compress.lzo;

import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @ClassName ReadLzoApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/29 9:54
 * @Version V1.0
 */
public class ReadLzoApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = ReadLzoApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(ReadLzoApp.class);

    /**
     * 实现Mapper类
     */
    public static class ReadLzoAppMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
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

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 从参数中获取输入路径
        Path inputDir = new Path(args[0]);
        // 如果输出路径不存在则报错
        if (!inputDir.getFileSystem(conf).exists(inputDir)) {
            LOG.error("==== 输入路径或输入文件不存在！ ====");
            System.exit(1);
        }
        // 从参数中获取输出路径
        Path outputDir = new Path(args[1]);
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 设置作业的输入类为Lzo
        job.setInputFormatClass(LzoTextInputFormat.class);
        // 设置作业的输出类为Text
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置作业的主程序
        job.setJarByClass(ReadLzoApp.class);
        // 设置Map端的实现类
        job.setMapperClass(ReadLzoAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置作为的Reducer数量为0
        job.setNumReduceTasks(0);
        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, inputDir);
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
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
        // 确保Map端的推测执行时关闭的
        conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
        // 如果core-site.xml没有配置的话，直接在代码中配置即可
        conf.set("io.compression.codecs",
                "org.apache.hadoop.io.compress.GzipCodec," +
                "org.apache.hadoop.io.compress.DefaultCodec," +
                "org.apache.hadoop.io.compress.BZip2Codec," +
                "com.hadoop.compression.lzo.LzoCodec," +
                "com.hadoop.compression.lzo.LzopCodec," +
                "org.apache.hadoop.io.compress.SnappyCodec");
        // 设置Lzo专用编解码实现
        conf.set("io.compression.codecs.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        int status = 0;
        try {
            status = ToolRunner.run(conf, new ReadLzoApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/mapreduce/lzo_out", "/apps/mapreduce/lzo2txt_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
