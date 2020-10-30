package cn.itcast.hadoop.mapreduce.compress.orcfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * @ClassName ReadOrcFileApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/29 18:21
 * @Version V1.0
 */
public class ReadOrcFileApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = ReadOrcFileApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(ReadOrcFileApp.class);

    /**
     * 实现Mapper类
     */
    public static class ReadOrcFileAppMapper extends Mapper<NullWritable, OrcStruct, NullWritable, Text> {
        private NullWritable outputKey;
        private Text outputValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputKey = NullWritable.get();
            outputValue = new Text();
        }
        public void map(NullWritable key, OrcStruct value, Context output) throws IOException, InterruptedException {
            this.outputValue.set(
                value.getFieldValue(0).toString()+","+
                value.getFieldValue(1).toString()+","+
                value.getFieldValue(2).toString()+","+
                value.getFieldValue(3).toString()+","+
                value.getFieldValue(4).toString()+","+
                value.getFieldValue(5).toString()+","+
                value.getFieldValue(6).toString()+","+
                value.getFieldValue(7).toString()
            );
            output.write(outputKey, outputValue);
        }
    }

    /**
     * 重写Tool接口的run方法，用于提交作业
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(ReadOrcFileApp.class);
        // 设置作业的输入为OrcInputFormat
        job.setInputFormatClass(OrcInputFormat.class);
        // 设置作业的输入路径
        OrcInputFormat.addInputPath(job, new Path(args[0]));
        // 设置作业的Mapper类
        job.setMapperClass(ReadOrcFileAppMapper.class);
        // 设置作业使用0个Reduce（直接从map端输出）
        job.setNumReduceTasks(0);
        // 设置作业的输入为TextOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);
        // 从参数中获取输出路径
        Path outputDir = new Path(args[1]);
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 设置作业的输出路径
        FileOutputFormat.setOutputPath(job, outputDir);
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
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000L);
        int status = 0;
        try {
            status = ToolRunner.run(conf, new ReadOrcFileApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/mapreduce/pay_orc_out", "/apps/mapreduce/pay_orc2txt_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }


}
