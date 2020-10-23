package cn.itcast.hadoop.mapreduce.text;

import cn.itcast.hadoop.mapreduce.bean.OrderBean;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 计算订单金额Top5
 * @ClassName OrderTop5App
 * @Description
 * @Created by MengYao
 * @Date 2020/10/22 10:10
 * @Version V1.0
 */
public class OrderTop5App extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = OrderTop5App.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(OrderTop5App.class);

    /**
     * 实现Mapper类
     */
    public static class OrderTop5AppMapper extends Mapper<LongWritable, Text, NullWritable, OrderBean> {
        private SortedMap<Double, OrderBean> top5 = new TreeMap<>();
        private NullWritable outputKey;
        private OrderBean outputValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            this.outputValue = OrderBean.of(value.toString());
            if (top5.size()>=5) {
                top5.remove(top5.firstKey());
            }
            top5.put(outputValue.getActualAmount(), outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            top5.forEach((k,v)->{
                try {
                    context.write(outputKey, v);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * 实现Reducer类
     */
    public static class OrderTop5AppReducer extends Reducer<NullWritable, OrderBean, NullWritable, Text> {
        private SortedMap<Double, OrderBean> top5 = new TreeMap<>();
        private NullWritable outputKey;
        private Text outputValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
            this.outputValue = new Text();
        }
        @Override
        protected void reduce(NullWritable key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
            Iterator<OrderBean> iterator = values.iterator();
            while(iterator.hasNext()) {
                OrderBean bean = iterator.next();
                outputValue.set(bean.getOrderSn()+"\t"+bean.getActualAmount());
                context.write(NullWritable.get(), outputValue);
            }
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
        job.setMapperClass(OrderTop5AppMapper.class);
        // 配置作业的Map函数所需的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 配置作业的Map函数所需的Value类型
        job.setMapOutputValueClass(OrderBean.class);
        // 配置作业的Reduce函数实现
        job.setReducerClass(OrderTop5AppReducer.class);
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
        int status = 0;
        try {
            status = ToolRunner.run(conf, new OrderTop5App(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data2/orders.csv", "/apps/mapreduce/top5_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
