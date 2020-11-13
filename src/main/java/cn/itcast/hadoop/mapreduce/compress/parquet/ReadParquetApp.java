package cn.itcast.hadoop.mapreduce.compress.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 将Parquet处理为普通文本文件
 * @ClassName ReadParquetApp
 * @Description
 * @Created by MengYao
 * @Date 2020/11/5 17:34
 * @Version V1.0
 */
public class ReadParquetApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = ReadParquetApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(ReadParquetApp.class);


    /**
     * 实现Mapper类
     *      K1=LongWritable,
     *      V1=Group (表示将读取到Parquet文件的每一行数据都解析为schema和data)
     */
    public static class ReadParquetAppMapper extends Mapper<LongWritable, Group, NullWritable, Text> {
        private NullWritable outputKey;
        private Text outputValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
            this.outputValue = new Text();
        }
        @Override
        protected void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            this.outputValue.set(value.toString());
            context.write(outputKey, outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
            this.outputValue = null;
        }
    }

    public static class ParquetReadSupport extends DelegatingReadSupport {
        public ParquetReadSupport() {
            super(new GroupReadSupport());
        }
        @Override
        public ReadContext init(InitContext context) {
            return super.init(context);
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
        // 设置作业的主程序
        job.setJarByClass(ReadParquetApp.class);
        // 设置Map端的实现类
        job.setMapperClass(ReadParquetAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置输入文件类型为Parquet
        job.setInputFormatClass(ParquetInputFormat.class);
        // 设置输入文件路径
        FileInputFormat.addInputPath(job, inputDir);
        // 设置如何解析输入数据
        ParquetInputFormat.setReadSupportClass(job, ParquetReadSupport.class);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputDir);
        // 设置作为的Reducer数量为0
        job.setNumReduceTasks(0);
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 创建作业
     * @param args
     * @return
     */
    public static int createJob(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
        // 设置MapReduce的运行模式为Local模式
        conf.set(MRConfig.FRAMEWORK_NAME, "local");
        // 设置MapReduce处理数据的文件系统
        conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://node1.itcast.cn:9820");
        int status = 0;
        try {
            status = ToolRunner.run(conf, new ReadParquetApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/mapreduce/parquet_out", "/apps/mapreduce/parquet2txt_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
