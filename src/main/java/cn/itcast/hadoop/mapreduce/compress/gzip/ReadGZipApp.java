package cn.itcast.hadoop.mapreduce.compress.gzip;

import cn.itcast.hadoop.mapreduce.compress.sequencefile.ReadSeqFileApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * 将输入的GZip文件处理为普通文件
 * @ClassName ReadGZipApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 10:47
 * @Version V1.0
 */
public class ReadGZipApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = ReadGZipApp.class.getSimpleName();

    /**
     * 实现Mapper类
     */
    static class ReadGZipAppMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
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

    /**
     * 实现Reduce类
     */
    static class ReadSeqFileAppReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private NullWritable outputKey;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
        }
        @Override
        protected void reduce(NullWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iterator = value.iterator();
            while (iterator.hasNext()) {
                context.write(outputKey, iterator.next());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputKey = null;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 实例化作业
        Job job = Job.getInstance(getConf(), JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(ReadSeqFileApp.class);
        // 设置Map端的实现类
        job.setMapperClass(ReadGZipAppMapper.class);
        // 设置Map端输入的Key类型
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置Map端输入的Value类型
        job.setMapOutputValueClass(Text.class);
        // 设置输入文件路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置作为的Reducer数量为0
        job.setNumReduceTasks(0);
        // 设置输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int createJob(String[] args) {
        Configuration conf = new Configuration();
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.set("dfs.datanode.socket.write.timeout", "7200000");
        // Map输入的最小Block应拆分为多大
        conf.set("mapreduce.input.fileinputformat.split.minsize", "268435456");
        // 启用压缩
        conf.set("mapreduce.map.output.compress", "true");
        // 设置压缩算法为GZip
        conf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class.getName());
        int status = 0;
        try {
            status = ToolRunner.run(conf, new ReadGZipApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }

}
