package cn.itcast.hadoop.mapreduce.compress.orcfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @ClassName WriteOrcFileApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/29 18:21
 * @Version V1.0
 */
public class WriteOrcFileApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = WriteOrcFileApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(WriteOrcFileApp.class);
    private static final String SCHEMA = "struct<id:string,type:string,orderID:string,bankCard:string,cardType:string,ctime:string,utime:string,remark:string>";

    /**
     * 实现Mapper类
     */
    public static class WriteOrcFileAppMapper extends Mapper<LongWritable, Text, NullWritable, OrcStruct> {
        private TypeDescription schema = TypeDescription.fromString(SCHEMA);
        private final NullWritable outputKey = NullWritable.get();
        private final OrcStruct outputValue = (OrcStruct) OrcStruct.createValue(schema);
        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",",8);
            outputValue.setFieldValue(0, new Text(fields[0]));
            outputValue.setFieldValue(1, new Text(fields[1]));
            outputValue.setFieldValue(2, new Text(fields[2]));
            outputValue.setFieldValue(3, new Text(fields[3]));
            outputValue.setFieldValue(4, new Text(fields[4]));
            outputValue.setFieldValue(5, new Text(fields[5]));
            outputValue.setFieldValue(6, new Text(fields[6]));
            outputValue.setFieldValue(7, new Text(fields[7]));
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
        // 设置Schema
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, SCHEMA);
        // 实例化作业
        Job job = Job.getInstance(conf, JOB_NAME);
        // 设置作业的主程序
        job.setJarByClass(WriteOrcFileApp.class);
        // 设置作业的Mapper类
        job.setMapperClass(WriteOrcFileAppMapper.class);
        // 设置作业的输入为TextInputFormat（普通文本）
        job.setInputFormatClass(TextInputFormat.class);
        // 设置作业的输出为OrcOutputFormat
        job.setOutputFormatClass(OrcOutputFormat.class);
        // 设置作业使用0个Reduce（直接从map端输出）
        job.setNumReduceTasks(0);
        // 设置作业的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 从参数中获取输出路径
        Path outputDir = new Path(args[1]);
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 设置作业的输出路径
        OrcOutputFormat.setOutputPath(job, outputDir);
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
        conf.set(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, "7200000");
        int status = 0;
        try {
            status = ToolRunner.run(conf, new WriteOrcFileApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data2/pay.csv", "/apps/mapreduce/pay_orc_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }
}