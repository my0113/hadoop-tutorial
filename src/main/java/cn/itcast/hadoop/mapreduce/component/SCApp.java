package cn.itcast.hadoop.mapreduce.component;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @ClassName SourceCodeApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/21 11:31
 * @Version V1.0
 */
public class SCApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = SCApp.class.getSimpleName();


    @Override
    public int run(String[] args) throws Exception {
//        MapFileOutputFormat
        // 创建作业实例
        Job job = Job.getInstance(getConf(), JOB_NAME);
        // 配置作业主类
        job.setJarByClass(this.getClass());
        // 配置输入处理为TextInputFormat
        job.setInputFormatClass(CustomInputFormat.class);
        // 配置作业的输入数据路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 配置作业的输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 配置作业的Map函数实现
        job.setMapperClass(null);
        // 配置作业的Map函数所需的Key类型
        job.setMapOutputKeyClass(Text.class);
        // 配置作业的Map函数所需的Value类型
        job.setMapOutputValueClass(LongWritable.class);
//        job
        // 配置作业的Map端Combiner实现（减少数据传输开销）
        job.setCombinerClass(null);
        // 配置作业的Reduce函数实现
        job.setReducerClass(null);
        // 配置作业的Reduce函数所需的Key类型
        job.setOutputKeyClass(Text.class);
        // 配置作业的Reduce函数所需的Value类型
        job.setOutputValueClass(LongWritable.class);
        // 提交作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }


}
