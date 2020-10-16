package cn.itcast.hadoop.mapreduce.db.imitate;

import cn.itcast.hadoop.mapreduce.db.imitate.config.CommandConfig;
import cn.itcast.hadoop.mapreduce.db.imitate.mapper.ExportMapper;
import cn.itcast.hadoop.mapreduce.db.imitate.mapper.ImportMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 一个模仿Sqoop的例子,仅支持MySQL(主要实现两个功能)
 *      【
 *          <b>注意</b>：外部数据源写入大数据生态系统（HDFS、HIVE、HBase）数据时叫做导入；从大数据生态系统写入到其他数据源时叫做导出
 *          1、将MySQL表数据导出入HDFS
 *              bin/sqoop import --connect jdbc:mysql://192.168.88.10:3306/test --username root --password 123456 --table users --target-dir /sqoop/tb1 -m 2
 *          2、将HDFS中的文件导出到MySQL表中
 *              bin/sqoop export --connect jdbc:mysql://192.168.88.10:3306/test --username root --password 123456 --table users --fields id,name,age,birthday --export-dir /testdata/users1/part-m-00000 --fields-terminated-by ',' -m 1
 *      】
 * @ClassName ImitateSqoopApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/14 10:42
 * @Version V1.0
 */
public class SqoopApp {

    private static final String APP_NAME = SqoopApp.class.getSimpleName();


    public static void main(String[] args) throws Exception {
        // 导出命令
        args = new String[] {
                "--export",
                "--connect", "jdbc:mysql://192.168.88.10:3306/test",
                "--username", "root",
                "--password", "123456",
                "--table", "test",
                "--fields", "id,name,age,birthday",
                "--exportDir", "/apps/mr/data",
                "--fieldsTerminatedBy", "\t",
                "-m","1"
        };
        // 导入命令
        args = new String[] {
                "--import",
                "--connect", "jdbc:mysql://192.168.88.10:3306/test",
                "--username", "root",
                "--password", "123456",
                "--table", "123456",
                "--targetDir", "/apps/mr/data",
                "-m","1"
        };
        // 提取命令行
        CommandConfig cmdConfig = extractCommand(args);
        // 构建作业并提交运行
        System.exit(createJob(cmdConfig));
    }

    /**
     * 解析命令行参数
     * @param args
     */
    private static CommandConfig extractCommand(String[] args) {
        // 定义命令
        Options options = new Options()
                .addOption("h","help", false, "查看帮助信息")
                .addOption(null,"import", false, "指定导入模式")
                .addOption(null,"export", false, "指定导出模式")
                .addOption(null,"connect", true, "指定MySQL连接URL")
                .addOption(null,"username", true, "指定MySQL访问用户")
                .addOption(null,"password", true, "指定MySQL访问用户的密码")
                .addOption(null,"table", true, "指定表名称")
                .addOption(null,"exportDir", true, "指定导出路径")
                .addOption(null,"targetDir", true, "指定导入路径")
                .addOption(null,"fieldsTerminatedBy", true, "指定分隔符")
                .addOption("m","map", true, "指定map任务数量")
                .addOption(null, "fields", true, "指定读取数据库字段")
                ;
        // 创建帮助信息
        HelpFormatter formatter = new HelpFormatter();
        try {
            // 解析命令
            CommandLineParser cli = new DefaultParser();
            CommandLine cmdLine = cli.parse(options, args);
            // 询问阶段
            if (cmdLine.hasOption("help") || cmdLine.hasOption("h")) {
                formatter.printHelp(APP_NAME, options, true);
                return null;
            }
            // 配置参数
            CommandConfig cmdConfig = CommandConfig.create();
            if (cmdLine.hasOption(CommandConfig.IMPORT)) {
                cmdConfig.setImport(true);
                if (cmdLine.hasOption(CommandConfig.CONNECT)) { cmdConfig.setConnect(cmdLine.getOptionValue(CommandConfig.CONNECT)); }
                if (cmdLine.hasOption(CommandConfig.USERNAME)) { cmdConfig.setUsername(cmdLine.getOptionValue(CommandConfig.USERNAME)); }
                if (cmdLine.hasOption(CommandConfig.PASSWORD)) { cmdConfig.setPassword(cmdLine.getOptionValue(CommandConfig.PASSWORD)); }
                if (cmdLine.hasOption(CommandConfig.TABLE)) { cmdConfig.setTable(cmdLine.getOptionValue(CommandConfig.TABLE)); }
                if (cmdLine.hasOption(CommandConfig.TARGET_DIR)) { cmdConfig.setTargetDir(cmdLine.getOptionValue(CommandConfig.TARGET_DIR)); }
                if (cmdLine.hasOption(CommandConfig.MAP)) { cmdConfig.setMap(Integer.parseInt(cmdLine.getOptionValue(CommandConfig.MAP))); }
                return cmdConfig;
            } else if (cmdLine.hasOption(CommandConfig.EXPORT)) {
                cmdConfig.setExport(true);
                if (cmdLine.hasOption(CommandConfig.CONNECT)) { cmdConfig.setConnect(cmdLine.getOptionValue(CommandConfig.CONNECT)); }
                if (cmdLine.hasOption(CommandConfig.USERNAME)) { cmdConfig.setUsername(cmdLine.getOptionValue(CommandConfig.USERNAME)); }
                if (cmdLine.hasOption(CommandConfig.PASSWORD)) { cmdConfig.setPassword(cmdLine.getOptionValue(CommandConfig.PASSWORD)); }
                if (cmdLine.hasOption(CommandConfig.TABLE)) { cmdConfig.setTable(cmdLine.getOptionValue(CommandConfig.TABLE)); }
                if (cmdLine.hasOption(CommandConfig.FIELDS)) { cmdConfig.setFields(cmdLine.getOptionValue(CommandConfig.FIELDS)); }
                if (cmdLine.hasOption(CommandConfig.EXPORT_DIR)) { cmdConfig.setExportDir(cmdLine.getOptionValue(CommandConfig.EXPORT_DIR)); }
                if (cmdLine.hasOption(CommandConfig.FIELD_TERMINATED_BY)) { cmdConfig.setFieldTerminatedBy(cmdLine.getOptionValue(CommandConfig.FIELD_TERMINATED_BY)); }
                if (cmdLine.hasOption(CommandConfig.MAP)) { cmdConfig.setMap(Integer.parseInt(cmdLine.getOptionValue(CommandConfig.MAP))); }
                return cmdConfig;
            } else {
                formatter.printHelp(APP_NAME, options, true);
            }
        } catch (ParseException e) {
            formatter.printHelp(APP_NAME, options, true);
        }
        return null;
    }

    private static int createJob(CommandConfig cmdConfig) throws Exception {
        Preconditions.checkNotNull(cmdConfig, "输出命令错误！");
        Configuration conf = new Configuration();
        /**
         * DBConfiguration.configureDB与如下设置效果相同
         * ====================================================================================
         * conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, cmdConfig.getDriverClass());
         * conf.set(DBConfiguration.URL_PROPERTY, cmdConfig.getConnect());
         * conf.set(DBConfiguration.USERNAME_PROPERTY, cmdConfig.getUsername());
         * conf.set(DBConfiguration.PASSWORD_PROPERTY, cmdConfig.getPassword());
         * ====================================================================================
         */
        DBConfiguration.configureDB(conf, cmdConfig.getDriverClass(), cmdConfig.getConnect(), cmdConfig.getUsername(), cmdConfig.getPassword());
        // 命令行中指定的map数量（不指定时默认为1）
        conf.setInt(MRJobConfig.NUM_MAPS, cmdConfig.getMap());
        // 无需使用Reducer
        conf.set(MRJobConfig.NUM_REDUCES, "0");
        // 构建MR作业
        Job job = Job.getInstance(conf, APP_NAME);
        // 设置作业的主程序
        job.setJarByClass(SqoopApp.class);
        // 设置作业Mapper的Key
        job.setMapOutputKeyClass(NullWritable.class);
        // 设置作业Mapper的Value
        job.setMapOutputValueClass(GeneralWritable.class);
        // 如果是导入
        if (cmdConfig.isImport()) {
            // 导入使用DBInputFormat
            job.setInputFormatClass(DBInputFormat.class);
            // 设置作业依赖的Mapper
            job.setMapperClass(ImportMapper.class);
            // 设置输出使用DBOutputFormat
            job.setOutputFormatClass(TextOutputFormat.class);
            // 设置作业输出路径
            FileOutputFormat.setOutputPath(job, new Path(cmdConfig.getTargetDir()));
            // 设置作业（从数据库读取）的输入参数
            DBInputFormat.setInput(
                    job,
                    GeneralWritable.class,
                    "SELECT * FROM "+cmdConfig.getTable(),
                    "SELECT COUNT(1) FROM "+cmdConfig.getTable());
        }
        // 如果是导出
        if (cmdConfig.isExport()) {
            // 导出使用TextInputFormat
            job.setInputFormatClass(TextInputFormat.class);
            // 设置输入路径
            FileInputFormat.setInputPaths(job, cmdConfig.getExportDir());
            // 设置作业依赖的Mapper
            job.setMapperClass(ExportMapper.class);
            // 设置输出使用DBOutputFormat
            job.setOutputFormatClass(DBOutputFormat.class);
            // 设置作业（写入到数据库）的输出参数
            DBOutputFormat.setOutput(job, cmdConfig.getTable(), cmdConfig.getFields());
        }
        // 提交MR作业并等待执行完成
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
