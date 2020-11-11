package cn.itcast.hadoop.mapreduce.flow;

import cn.itcast.hadoop.mapreduce.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 流量分析案例
 */
public class FlowApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = FlowApp.class.getSimpleName();
    private static final Logger LOG = LoggerFactory.getLogger(FlowApp.class);

    /**
     * 实现Mapper类
     */
    public static class FlowAppMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1:拆分行文本数据（拆分v1）
            String[] split = value.toString().split("\t");
            //2:从拆分数组中得到手机号，得到K2
            String phoneNum = split[1];
            //3:从拆分数组中得到4个流量字段，并封装到FlowBean,得到V2
            FlowBean flowBean = new FlowBean();
            flowBean.setUpFlow(Integer.parseInt(split[6]));
            flowBean.setDownFlow(Integer.parseInt(split[7]));
            flowBean.setUpCountFlow(Integer.parseInt(split[8]));
            flowBean.setDownCountFlow(Integer.parseInt(split[9]));
            //4:将K2和V2写入上下文中
            context.write(new Text(phoneNum), flowBean);
        }
    }

    /**
     * 实现Reducer类
     */
    public static class FlowAppReducer extends Reducer<Text, FlowBean,Text,FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            //1:定义四个变量，分别来存储上行包数，下行包数，上行流量总和，下行流量总和
            int upFlow = 0;
            int downFlow = 0;
            int upCountFlow = 0;
            int downCountFlow = 0;
            //2:遍历集合，将集合中每一个FlowBean的四个流量字段相加
            for (FlowBean flowBean : values) {
                upFlow += flowBean.getUpFlow();
                downFlow += flowBean.getDownFlow();
                upCountFlow += flowBean.getUpCountFlow();
                downCountFlow += flowBean.getDownCountFlow();
            }
            //3:K3就是原来的K2，V3就是新的FlowBean
            FlowBean flowBean = new FlowBean(upFlow, downFlow, upCountFlow, downCountFlow);
            //4:将K3和V3写入上下文中
            context.write(key, flowBean);
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
        Configuration configuration = new Configuration();
        //1、创建建一个job任务对象
        Job job = Job.getInstance(configuration, JOB_NAME);
        //2、指定job所在的jar包
        job.setJarByClass(FlowApp.class);
        //3、指定源文件的读取方式类和源文件的读取路径
        job.setInputFormatClass(TextInputFormat.class); //按照行读取
        TextInputFormat.addInputPath(job, new Path(args[0])); //只需要指定源文件所在的目录即可
//        TextInputFormat.addInputPath(job, new Path("file:///E:\\input\\flowcount")); //只需要指定源文件所在的目录即可

        //4、指定自定义的Mapper类和K2、V2类型
        job.setMapperClass(FlowAppMapper.class); //指定Mapper类
        job.setMapOutputKeyClass(Text.class); //K2类型
        job.setMapOutputValueClass(FlowBean.class);//V2类型

        //5、指定自定义分区类（如果有的话）
        //6、指定自定义分组类（如果有的话）
        //7、指定自定义Combiner类(如果有的话)

        //设置ReduceTask个数

        //8、指定自定义的Reducer类和K3、V3的数据类型
        job.setReducerClass(FlowAppReducer.class); //指定Reducer类
        job.setOutputKeyClass(Text.class); //K3类型
        job.setOutputValueClass(FlowBean.class);  //V3类型

        //9、指定输出方式类和结果输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        // 从参数中获取输出路径
        Path outputDir = new Path(args[1]);
        // 如果输出路径已存在则删除
        outputDir.getFileSystem(conf).delete(outputDir, true);
        // 配置作业的输出数据路径
        TextOutputFormat.setOutputPath(job, outputDir); //目标目录不能存在，否则报错
//        TextOutputFormat.setOutputPath(job, new  Path("file:///E:\\output\\flowcount")); //目标目录不能存在，否则报错

        //10、将job提交到yarn集群
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
            status = ToolRunner.run(conf, new FlowApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        // 测试使用（第一个参数对应的文件必须在HDFS中存在）
        args = new String[] {"/apps/data3/data_flow.dat", "/apps/mapreduce/flow_out"};
        if (args.length!=2) {
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH> <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }
}
