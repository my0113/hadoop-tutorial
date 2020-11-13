package cn.itcast.hadoop.mapreduce.compress.parquet;

import cn.itcast.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.DelegatingWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

/**
 * 将文本文件处理为Parquet
 *      注意：
 *          必须确保在Hadoop集群的classpath中有parquet-hadoop-bundle-1.10.0.jar，否则会提示找不到类异常：ClassNotFoundException: org.apache.parquet.column.values.bitpacking.LemireBitPackingBE
 * @ClassName WriteParquetApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/30 17:58
 * @Version V1.0
 */
public class WriteParquetApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = WriteParquetApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(WriteParquetApp.class);
    private static final String SCHEMA_KEY = "parquet.example.schema";


    /**
     * 实现Mapper类
     */
    public static class WriteParquetAppMapper extends Mapper<LongWritable, Text, Void, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(null, value);
        }
    }

    /**
     * 负责解析Mapper或Reducer中输出的Key-Value数据为Parquet（Key是Void格式无需处理，主要是处理Value）
     * @param <T>
     */
    public static class ParquetWriteSupport<T> extends WriteSupport<T> {
        private RecordConsumer recordConsumer;
        private MessageType schema;
        @Override
        public WriteContext init(Configuration conf) {
            this.schema = parseSchema(conf);
            Preconditions.checkNotNull(schema, "未指定数据文件Schema信息！");
            return new WriteContext(schema, new HashMap<>());
        }
        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }
        @Override
        public void write(T record) {
            recordConsumer.startMessage();
            String[] values = record.toString().split("##");
            List<ColumnDescriptor> columns = schema.getColumns();
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                recordConsumer.startField(columns.get(i).getPath()[0], i);
                recordConsumer.addBinary(Binary.fromCharSequence(value));
                recordConsumer.endField(columns.get(i).getPath()[0], i);
            }
            recordConsumer.endMessage();
        }
        private MessageType parseSchema(Configuration conf) {
            String schema = conf.get(SCHEMA_KEY);
            StringTokenizer tokenizer = new StringTokenizer(schema, ParquetTool.FIELD_SEPERATOR);
            Types.MessageTypeBuilder builder = Types.buildMessage();
            while (tokenizer.hasMoreTokens()) {
                builder.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                        .named(tokenizer.nextToken().trim());
            }
            return builder.named("root");
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
        job.setJarByClass(WriteParquetApp.class);
        // 设置Map端的实现类
        job.setMapperClass(WriteParquetAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置输入InputFormat
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输入文件路径
        FileInputFormat.addInputPath(job, inputDir);
        // 设置输出OutputFormat
        job.setOutputFormatClass(ParquetOutputFormat.class);
        // 设置如何解析输出数据
        ParquetOutputFormat.setWriteSupportClass(job, ParquetWriteSupport.class);
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
//        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
//        // 设置MapReduce的运行模式为Local模式
//        conf.set(MRConfig.FRAMEWORK_NAME, "local");
//        // 设置MapReduce处理数据的文件系统
//        conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://node1.itcast.cn:9820");
        // 设置Parquet格式所需的Schema
        conf.set(SCHEMA_KEY, ParquetTool.getSchema(OrderBean.class));
        int status = 0;
        try {
            status = ToolRunner.run(conf, new WriteParquetApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data2/orders.csv", "/apps/mapreduce/parquet_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
