package cn.itcast.hadoop.mapreduce.compress.lzo;

import cn.itcast.hadoop.mapreduce.compress.sequencefile.ReadSeqFileApp;
import io.airlift.compress.lzo.LzoCodec;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @ClassName WriteLzoApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 16:33
 * @Version V1.0
 */
public class WriteLzoApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = WriteLzoApp.class.getSimpleName();

    @Override
    public int run(String[] args) throws Exception {
        // 实例化作业
        Job job = Job.getInstance(getConf(), JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(ReadSeqFileApp.class);
        // 设置Map端的实现类
        job.setMapperClass(null);
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
        // 设置启用输出压缩
        FileOutputFormat.setCompressOutput(job, true);
        // 设置输出压缩格式为Lzo
        FileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
        // 提交作业并等待执行完成
        int status = job.waitForCompletion(true) ? 0 : 1;
        // 为输出的Lzo文件添加索引
//        LzoIndexer lzoIndexer = new LzoIndexer(conf);
//        lzoIndexer.index(new Path(args[1]));
        return status;
    }
}
