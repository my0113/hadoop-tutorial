package cn.itcast.hadoop.mapreduce.text;

import cn.itcast.hadoop.mapreduce.bean.OrderBean;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 计算PDD订单平均金额
 * @ClassName OrderAvgApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/27 14:56
 * @Version V1.0
 */
public class OrderAvgApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = OrderAvgApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(OrderAvgApp.class);

    /**
     * 实现Mapper类
     */
    public static class OrderAvgAppMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private NullWritable outputKey;
        private Text outputValue;
        private OrderBean bean;
        private long count;
        private double sum;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputKey = NullWritable.get();
            outputValue = new Text();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            bean = OrderBean.of(value.toString());
            sum += bean.getActualAmount();
            count++;
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputValue.set(count+","+sum);
            context.write(outputKey, outputValue);
        }
    }

    /**
     * 实现Reducer类
     */
    public static class OrderAvgAppReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private NullWritable outputKey;
        private Text outputValue;
        private long count;
        private double sum;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
            this.outputValue = new Text();
        }
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while(iterator.hasNext()) {
                Text next = iterator.next();
                String[] fields = next.toString().split(",",2);
                count += Long.parseLong(fields[0]);
                sum += Double.parseDouble(fields[1]);
            }
            outputValue.set(StringUtils.EMPTY+(sum/count));
            context.write(outputKey, outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
            this.outputValue = null;
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
        // 创建作业实例
        Job job = Job.getInstance(conf, JOB_NAME);
        String appDir = null;
        if ((appDir = conf.get("windows.app.dir"))!=null) {
            job.setJar(appDir);
        }
        // 配置作业主类
        job.setJarByClass(this.getClass());
        // 配置输入处理为TextInputFormat
        job.setInputFormatClass(TextInputFormat.class);
        // 配置作业的输入数据路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 从参数中获取输出路径
        Path outputDir = new Path(args[1]);
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 配置作业的输出数据路径
        FileOutputFormat.setOutputPath(job, outputDir);
        // 配置作业的Map函数实现
        job.setMapperClass(OrderAvgAppMapper.class);
        // 配置作业的Map函数所需的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 配置作业的Map函数所需的Value类型
        job.setMapOutputValueClass(Text.class);
        // 配置作业的Reduce函数实现
        job.setReducerClass(OrderAvgAppReducer.class);
        // 配置作业的Reduce函数所需的Key类型
        job.setOutputKeyClass(NullWritable.class);
        // 配置作业的Reduce函数所需的Value类型
        job.setOutputValueClass(Text.class);
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
        // 如果是从Windows提交
        if(SystemUtils.IS_OS_WINDOWS) {
            // 加载并使用集群中的yarn-site.xml、scheduler.xml配置
            conf.addResource(new YarnConfiguration());
            // 模拟root用户提交应用程序
            System.setProperty("HADOOP_USER_NAME", "root");
            // 如果为true时，用户可以跨平台提交应用程序，即从Windows客户端向Linux/Unix服务器提交应用程序
            conf.set("mapreduce.app-submission.cross-platform", "true");
            // 设置本地jar包路径
            conf.set("windows.app.dir", "D:\\works\\ITCAST\\codes\\hadoop-tutorial\\target\\hadoop-tutorial-1.0-SNAPSHOT.jar");
        }
        int status = 0;
        try {
            status = ToolRunner.run(conf, new OrderAvgApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data2/orders.csv", "/apps/mapreduce/avg_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
