package cn.itcast.hadoop.mapreduce.db;

import cn.itcast.hadoop.mapreduce.bean.ProductBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 读取普通文件输出到数据库
 * @ClassName WriteDBApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 15:31
 * @Version V1.0
 */
public class WriteDBApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = WriteDBApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(WriteDBApp.class);


    /**
     * 实现Mapper类
     */
    public static class WriteDBAppMapper extends Mapper<LongWritable, Text, NullWritable, ProductBean> {

        private NullWritable outputKey;
        private ProductBean outputValue;

        @Override
        protected void setup(
                Mapper<LongWritable, Text, NullWritable, ProductBean>.Context context)
                throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
            this.outputValue = new ProductBean();
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, NullWritable, ProductBean>.Context context)
                throws IOException, InterruptedException {
            //插入数据库成功的计数器
            final Counter successCounter = context.getCounter("exec", "successfully");
            //插入数据库失败的计数器
            final Counter faildCounter = context.getCounter("exec", "faild");
            //获取DFS上已有的机构化数据
            final String[] fields = value.toString().split("\t");
            //DBOutputFormatApp这个MapReduce应用导出的数据包含long类型的key，所以忽略key从1开始
            if (fields.length > 5) {
                long id = Long.parseLong(fields[1]);
                String name = fields[2];
                String model = fields[3];
                String color = fields[4];
                double price = Double.parseDouble(fields[5]);
                this.outputValue.set(id, name, model, color, price);
                context.write(outputKey, outputValue);
                //如果插入数据库成功则递增1，表示成功计数
                successCounter.increment(1L);
            } else {
                //如果插入数据库失败则递增1，表示失败计数
                faildCounter.increment(1L);
            }
        }
    }

    /**
     * DBOutputFormatReducer
     *         输入的key即DBOutputFormatMapper输出的key
     *         输出的value即DBOutputFormatMapper输出的value
     *         输出的key必须是继承自DBWritable的类型（DBOutputFormat要求输出的key必须是继承自DBWritable的类型）
     *         输出的value无所谓，这里我使用的是NullWritable作为占位输出
     * @author mengyao
     *
     */
    public static class DBOutputFormatReducer extends Reducer<NullWritable, ProductBean, ProductBean, NullWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<ProductBean> value,
                              Reducer<NullWritable, ProductBean, ProductBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
            for (ProductBean productWritable : value) {
                context.write(productWritable, key);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //在创建Configuration对象时紧跟着配置当前作业需要使用的JDBC配置
        DBConfiguration.configureDB(
                conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.104:3306/test?useUnicode=true&characterEncoding=utf8",
                "root",
                "123456");
        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(WriteDBApp.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);

        job.setMapperClass(WriteDBAppMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(ProductBean.class);

        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        job.setReducerClass(DBOutputFormatReducer.class);
        job.setOutputKeyClass(ProductBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        //配置当前作业输出到数据库的表、字段信息
        DBOutputFormat.setOutput(job, "product", new String[]{"id", "name", "model", "color", "price"});

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int createJob(String[] args) {
        Configuration conf = new Configuration();
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
        int status = 0;
        try {
            status = ToolRunner.run(conf, new ReadDBApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        if (args.length!=1) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }
}
