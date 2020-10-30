package cn.itcast.hadoop.mapreduce.join;

import cn.itcast.hadoop.mapreduce.tools.DFSTool;
import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用MapReduce在Mapper Side Join的案例
 *      【1、使用订单信息表的payId关联支付信息表的id】
 *
 * @ClassName MapperJoinApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 18:51
 * @Version V1.0
 */
public class MapperJoinApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = MapperJoinApp.class.getSimpleName();
    // 编码
    private static final String ENCODING = "encoding";

    /**
     * 实现Mapper类
     */
    static class MapperJoinAppMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        // 存储支付信息
        private final Map<String, String> payInfo = new HashMap<>();
        private Text outputKey;
        private NullWritable outputValue;
        private String delim = ",";
        @Override
        protected void setup(Mapper<LongWritable, Text, Text,NullWritable>.Context context) throws IOException, InterruptedException {
            // 从分布式缓存中获取文件地址（支付信息表），与conf.getStrings(MRJobConfig.CACHE_FILES)等值
            URI[] files = context.getCacheFiles();
            // 加载文件到Map任务端
            List<String> lines = DFSTool.readText(context.getConfiguration(), files[0].getPath());
            // 存放到map中
            lines.forEach(line -> {
                String[] fields = line.split(",", 8);
                // key=payId, value=除了payId以外的其他字段
                payInfo.put(fields[0], fields[1] +delim+ fields[2] +delim+ fields[3] +delim+ fields[4] +delim+ fields[5] +delim+ fields[6] +delim+ fields[7]);
            });
            this.outputKey = new Text();
            this.outputValue = NullWritable.get();
        }
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException,
                InterruptedException {
            String[] fields = value.toString().split("##", 48);
            // 获取到订单信息表中的payId
            String payId = fields[38];
            // 如果支付信息表包括订单信息表的payId
            if (payInfo.containsKey(payId)) {
                // 进行关联
                this.outputKey.set(value.toString()+delim+payInfo.get(payId));
            }
            // 输出数据
            context.write(outputKey, outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.payInfo.clear();
            this.outputKey = null;
            this.outputValue = null;
            this.delim = null;
        }
    }

    /**
     * 实现Reducer类
     */
    static class MapperJoinAppReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values,Context context)throws IOException,InterruptedException{
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 添加只读文件到分布式缓存，与conf.set(MRJobConfig.CACHE_FILES)等值
        job.addCacheFile(new Path(args[1]).toUri());
        // 设置作业的主程序
        job.setJarByClass(this.getClass());
        // 设置作业的输入为TextInputFormat（普通文本）
        job.setInputFormatClass(TextInputFormat.class);
        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置Map端的实现类
        job.setMapperClass(MapperJoinAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(NullWritable.class);
        // 设置Reduce端的实现类
        job.setReducerClass(MapperJoinAppReducer.class);
        // 设置Reduce数量
        job.setNumReduceTasks(1);
        // 设置Reduce端输出的Key类型
        job.setOutputKeyClass(Text.class);
        // 设置Reduce端输出的Value类型
        job.setOutputValueClass(NullWritable.class);
        // 设置作业的输出为TextInputFormat（普通文本）
        job.setOutputFormatClass(TextOutputFormat.class);
        // 从参数中获取输出路径
        Path outputPath = new Path(args[2]);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputPath);
        // 如果输出路径不为空则清空
        outputPath.getFileSystem(getConf()).delete(outputPath, true);
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
        // 设置字符集
        conf.set(ENCODING, "UTF-8");
        int status = 0;
        try {
            status = ToolRunner.run(conf, new MapperJoinApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（前两个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data2/orders.csv", "/apps/data2/pay.csv", "/apps/mapreduce/mapside_join"};
        if (args.length!=3) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <BIG_FILE_INPUT_PATH> <SMALL_FILE_INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
