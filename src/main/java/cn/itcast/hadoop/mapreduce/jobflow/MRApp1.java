package cn.itcast.hadoop.mapreduce.jobflow;

import cn.itcast.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 第一个MR（对PDD订单先按照天进行汇总）
 * @ClassName MRApp1
 * @Description
 * @Created by MengYao
 * @Date 2020/10/15 18:05
 * @Version V1.0
 */
public class MRApp1 {

    // 作业名称
    private static final String JOB_NAME = MRApp1.class.getSimpleName();
    // 行数据分隔符
    private static final String DELIMITER = "delimiter";

    /**
     * 实现Mapper类
     */
    static class MRApp1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text outputKey;
        private DoubleWritable outputValue;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            this.outputKey = new Text();
            this.outputValue = new DoubleWritable();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            OrderBean bean = OrderBean.of(value.toString());
            String ctime = bean.getCtime();
            double actualAmount = bean.getActualAmount();
            if (ctime.matches("(\\d{4}-\\d{2}-\\d{2}\\s{1}\\d{2}:\\d{2}:\\d{2})")) {
                outputValue.set(actualAmount);
                outputKey.set(ctime.substring(0, 10));
                // 输出格式为：outputKey=yyyy-MM-dd，outputValue=单笔订单实付金额
                context.write(outputKey, outputValue);
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
            this.outputValue = null;
        }
    }

    /**
     * 实现Reducer类
     */
    static class MRApp1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable outputValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputValue = new DoubleWritable();
        }
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.D;
            Iterator<DoubleWritable> iterator = values.iterator();
            // 计算日销售额
            while(iterator.hasNext()) {
                sum += iterator.next().get();
            }
            outputValue.set(sum);
            // 输出格式为：outputKey=yyyy-MM-dd，outputValue=日所有订单实付总金额
            context.write(key, outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputValue = null;
        }
    }

    /**
     * 创建作业
     * @param args
     * @return
     */
    public static Job createJob(Configuration conf, String[] args) throws Exception {
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
        // 设置自定义分隔符
        conf.set(DELIMITER, "##");
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(MRApp1.class);
        // 设置作业的输入为TextInputFormat（普通文本）
        job.setInputFormatClass(TextInputFormat.class);
        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置Map端的实现类
        job.setMapperClass(MRApp1Mapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(DoubleWritable.class);
        // 设置Reduce端的实现类
        job.setReducerClass(MRApp1Reducer.class);
        // 设置Reduce端输出的Key类型
        job.setOutputKeyClass(Text.class);
        // 设置Reduce端输出的Value类型
        job.setOutputValueClass(DoubleWritable.class);
        // 设置作业的输出为TextInputFormat（普通文本）
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业并等待执行完成
        return job;
    }

}
