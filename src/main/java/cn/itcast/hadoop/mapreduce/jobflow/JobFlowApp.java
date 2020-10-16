package cn.itcast.hadoop.mapreduce.jobflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

/**
 * MR工作流，流程如下
 *      【1、先计算日订单实付总金额。Job1的输入数据：orders.csv】
 *      【2、再计算月订单实付总金额。Job2的输入数据：Job1的输出】
 * @ClassName JobFlowApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/14 10:01
 * @Version V1.0
 */
public class JobFlowApp {

    // 作业工作流名称
    private static final String JOB_FLOW_NAME = JobFlowApp.class.getSimpleName();


    public static void main(String[] args) throws Exception {
        if (args.length!=3) {
            System.out.println("Usage: "+JOB_FLOW_NAME+" Input parameters <INPUT_PATH1> <OUTPUT_PATH2> <INPUT_PATH3>");
            System.exit(-1);
        }
        try {
            // 初始化配置
            Configuration conf = new Configuration();
            // 构建subJob1（日销售额）
            Job subJob1 = MRApp1.createJob(conf, args);
            // 构建ctrJob1控制器
            ControlledJob ctrJob1 = new ControlledJob(conf);
            // 配置ctrJob1实际运行subJob1
            ctrJob1.setJob(subJob1);
            // 构建subJob2（月销售额）
            Job subJob2 = MRApp2.createJob(conf, args);
            // 构建ctrJob2控制器
            ControlledJob ctrJob2 = new ControlledJob(conf);
            // 配置ctrJob2实际运行subJob2
            ctrJob2.setJob(subJob2);
            // 配置ctrJob2依赖ctrJob1
            ctrJob2.addDependingJob(ctrJob1);
            // 创建主控制器
            final JobControl jobControl = new JobControl(JOB_FLOW_NAME);
            // 配置ctrJob1加入主控制器
            jobControl.addJob(ctrJob1);
            // 配置ctrJob2加入主控制器
            jobControl.addJob(ctrJob2);
            // 配置主控制器的行为
            Thread thread = new Thread(jobControl) {
                @Override
                public synchronized void start() {
                    while (true) {
                        if (jobControl.allFinished()) {
                            System.out.println(jobControl.getSuccessfulJobList());
                            jobControl.stop();
                            break;
                        }
                    }
                }
            };
            // 提交运行主控制器
            thread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
