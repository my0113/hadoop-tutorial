package cn.itcast.hadoop.mapreduce.uber;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 这是一个测试UBer模式的WordCount例子
 *      【有些作业的输入文件很多但总体的数据量并不大，此时就无须切分过的分片，因为这会导致资源的浪费（内存、磁盘、YARN频繁的创建和销毁容器以及作业自身的时间延迟），UBer模式是用于解决这个问题的内置方案。】
 *
 * @ClassName UBerApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/12 15:26
 * @Version V1.0
 */
public class UBerApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = UBerApp.class.getSimpleName();
    // 行数据分隔符
    private static final long DFS_BLOCK_BYTES = 268435456L;
    private static final int MAX_MAP_NUMBER = 9;


    /**
     * 实现Mapper类
     */
    static class UBerAppMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text outputKey;
        private LongWritable outputValue;
        @Override
        protected void setup(Context context) {
            this.outputKey = new Text();
            this.outputValue = new LongWritable(1L);
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            final String[] words = value.toString().split(" ");
            for (String word : words) {
                this.outputKey.set(word);
                context.write(this.outputKey, this.outputValue);
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputKey = null;
            outputValue = null;
        }
    }

    /**
     * 实现Reducer类
     */
    static class UBerAppReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private Text outputKey;
        private LongWritable outputValue;
        @Override
        protected void setup(Context context) {
            this.outputKey = new Text();
            this.outputValue = new LongWritable();
        }
        @Override
        protected void reduce(Text key, Iterable<LongWritable> value, Context context)
                throws IOException, InterruptedException {
            long count = 0L;
            for (LongWritable item : value) {
                count += item.get();
            }
            this.outputKey.set(key);
            this.outputValue.set(count);
            context.write(this.outputKey, this.outputValue);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputKey = null;
            outputValue = null;
        }
    }

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
        // 如果输出路径不为空则清空
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 配置作业的输出数据路径
        FileOutputFormat.setOutputPath(job, outputDir);
        // 配置作业的Map函数实现
        job.setMapperClass(UBerAppMapper.class);
        // 配置作业的Map函数所需的Key类型
        job.setMapOutputKeyClass(Text.class);
        // 配置作业的Map函数所需的Value类型
        job.setMapOutputValueClass(LongWritable.class);
        // 配置作业的Reduce函数实现
        job.setReducerClass(UBerAppReducer.class);
        // 配置作业的Reduce函数所需的Key类型
        job.setOutputKeyClass(Text.class);
        // 配置作业的Reduce函数所需的Value类型
        job.setOutputValueClass(LongWritable.class);
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
        /**
         * 启用uber模式
         */
        conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, true);
        /**
         * 设置Mapper数量（默认值是9），超过这个阈值，就会认为任务太大而无法进行ubertask优化。
         */
        conf.setInt(MRJobConfig.JOB_UBERTASK_MAXMAPS, MAX_MAP_NUMBER);
        /**
         * 设置Reducer数量的阈值（默认值是1），超过这个阈值，就会认为任务对于ubertask优化来说太大了。仅支持0或1。
         * 超过这两个值的配置将会被忽略。
         */
        conf.setInt(MRJobConfig.JOB_UBERTASK_MAXREDUCES, 1);
        /**
         * 输入字节数的阈值，超过该阈值，作业对于超级任务优化来说被认为太大。如果未指定任何值，则将dfs.block.size用作默认值。
         * 如果底层的文件系统不是HDFS，必须保证mapred-site.xml中指定dfs.block.size的默认值。
         * 底层的文件系统是HDFS，设置为为256MB（默认是128MB）
         */
        conf.setLong(MRJobConfig.JOB_UBERTASK_MAXBYTES, DFS_BLOCK_BYTES);
        /**
         * 设置文件切分大小（单位：字节）始终按照UBer模式允许的最小设置
         */
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, DFS_BLOCK_BYTES / (MAX_MAP_NUMBER-1));
        int status = 0;
        try {
            status = ToolRunner.run(conf, new UBerApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[]{"/apps/data1/*", "/apps/mapreduce/uber_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
