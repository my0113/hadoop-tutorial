package cn.itcast.hadoop.mapreduce.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用MapReduce在Reducer Side Join的案例
 *      【1、使用订单信息表的payId关联支付信息表的id】
 *
 * @ClassName ReducerJoinApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 18:51
 * @Version V1.0
 */
public class ReducerJoinApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = ReducerJoinApp.class.getSimpleName();

    /**
     * 实现加载订单信息表的Mapper类
     */
    static class ReducerJoinAppOrderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey;
        private Text outputValue;
        private String delim;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = new Text();
            this.outputValue = new Text();
            this.delim = "##";
        }
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String record = value.toString();
            String[] fields = record.split(delim, 48);
            // 获取到订单信息表的payId
            // String payId = fields[38];
            // 获取订单编号
            String orderSn = fields[1];
            this.outputKey.set(orderSn);
            this.outputValue.set(record);
            // 输出格式为Key=payId，Value=value.toString
            context.write(outputKey, outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
            this.outputValue = null;
            this.delim = null;
        }
    }

    /**
     * 实现加载支付信息表的Reducer类
     */
    static class ReducerJoinAppPayMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey;
        private Text outputValue;
        private String delim;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = new Text();
            this.outputValue = new Text();
            this.delim = ",";
        }
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(delim, 8);
            // 设置Key=payId
            this.outputKey.set(fields[2]);
            // 设置Value=除了payId以外的其他字段
            this.outputValue.set(fields[1] +delim+ fields[2] +delim+ fields[3] +delim+ fields[4] +delim+ fields[5] +delim+ fields[6] +delim+ fields[7]);
            // 输出Key=payId，Value=除了payId以外的其他字段
            context.write(outputKey, outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
            this.outputValue = null;
            this.delim = null;
        }
    }

    /**
     * 实现Reducer类
     */
    static class ReduceJoinAppReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey;
        private Text outputValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = new Text();
            this.outputValue = new Text();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                context.write(key, iterator.next());
                
            }
//            while (iterator.hasNext()) {
//                String line = iterator.next().toString();
//                // 左表（订单信息）
//                if (!line.contains("ALIPAY")||!line.contains("WEIXIN")) {
//                    this.outputKey.set(line);
//                }
//            }
//            while (iterator.hasNext()) {
//                String line = iterator.next().toString();
//                // 右表（支付信息）
//                if (line.contains("ALIPAY")||line.contains("WEIXIN")) {
//                    this.outputValue.set(line);
//                    context.write(outputKey, outputValue);
//                }
//            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
            this.outputValue = null;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(this.getClass());
        // 设置作业的输入为TextInputFormat（普通文本）
        job.setInputFormatClass(TextInputFormat.class);
        // 从订单信息表Mapper中输入数据
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReducerJoinAppOrderMapper.class);
        // 从支付信息表Mapper中输入数据
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReducerJoinAppPayMapper.class);
        // 设置Reduce端的实现类
        job.setReducerClass(ReduceJoinAppReducer.class);
        // 设置Reduce数量
        job.setNumReduceTasks(1);
        // 设置Reduce端输出的Key类型
        job.setOutputKeyClass(Text.class);
        // 设置Reduce端输出的Value类型
        job.setOutputValueClass(Text.class);
        // 设置作业的输出为TextInputFormat（普通文本）
        job.setOutputFormatClass(TextOutputFormat.class);
        // 从参数中获取输出路径
        Path outputPath = new Path(args[2]);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputPath);
        // 如果输出路径不为空则清空
        outputPath.getFileSystem(conf).delete(outputPath, true);
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
        int status = 0;
        try {
            status = ToolRunner.run(conf, new ReducerJoinApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（前两个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data2/orders.csv", "/apps/data2/pay.csv", "/apps/mapreduce/reduceside_join"};
        if (args.length!=3) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <BIG_FILE_INPUT_PATH> <SMALL_FILE_INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}

