package cn.itcast.hadoop.mapreduce.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用DBInputFormat类读取数据库并将结果数据写到HDFS的/mapreduces/dboutput目录下，
 *         /mapreduces/dboutput/_SUCCESS：_SUCCESS空文件表示作业执行成功
 *         /mapreduces/dboutput/part-r-00000：文件表示作业的结果，内容如下：
 * 这个作业没有Reducer类，在默认的MapReduce作业中，如果输出的key，value是默认的LongWritable, Text，则Reducer类可以省略，省略不写时则默认启动一个Reducer
 *
 * 一定要记住在使用MapReduce操作数据库时一定要添加JDBC驱动jar包到Hadoop的classpath中，否则会报无法加载JDBC Driver类异常，如下：
 *       1、我这里添加到/usr/local/installs/hadoop/share/hadoop/mapreduce/lib/mysql-connector-java-5.1.26-bin.jar这里了，务必要重启集群使classpath生效。
 *       2、将JDBC驱动jar包打包到这个MapReduce作业jar包中。
 *
 * @author mengyao
 *
 */
/**
 * 从数据库读取数据输出为普通文件
 * @ClassName ReadDBApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 14:07
 * @Version V1.0
 */
public class ReadDBApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = ReadDBApp.class.getSimpleName();

    /**
     * 这个JavaBean需要实现Hadoop的序列化接口Writable和与数据库交互时的序列化接口DBWritable
     * 官方API中解释如下：
     *         public class DBInputFormat<T extends DBWritable>
     *             extends InputFormat<LongWritable, T> implements Configurable
     *             即Mapper的Key是LongWritable类型，不可改变；Value是继承自DBWritable接口的自定义JavaBean
     *
     * @author mengyao
     *
     */
    public static class ProductWritable implements Writable, DBWritable {

        private long id;            // bigint(20) NOT NULL AUTO_INCREMENT,
        private String name;        // varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '商品名称',
        private String model;        // varchar(30) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '型号',
        private String color;        // varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '颜色',
        private double price;        // decimal(10,0) DEFAULT NULL COMMENT '售价',

        @Override
        public void write(PreparedStatement ps) throws SQLException {
            ps.setLong(1, id);
            ps.setString(2, name);
            ps.setString(3, model);
            ps.setString(4, color);
            ps.setDouble(5, price);
        }

        @Override
        public void readFields(ResultSet rs) throws SQLException {
            this.id = rs.getLong(1);
            this.name = rs.getString(2);
            this.model = rs.getString(3);
            this.color = rs.getString(4);
            this.price = rs.getDouble(5);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = in.readLong();
            this.name = in.readUTF();
            this.model = in.readUTF();
            this.color = in.readUTF();
            this.price = in.readDouble();
        }

        @Override
        public void write(DataOutput output) throws IOException {
            output.writeLong(id);
            output.writeUTF(name);
            output.writeUTF(model);
            output.writeUTF(color);
            output.writeDouble(price);
        }

        @Override
        public String toString() {
            return id +"\t"+ name +"\t"+ model +"\t"+ color +"\t"+ price;
        }

    }

    /**
     * 实现Mapper类
     */
    public static class ReadDBAppMapper extends Mapper<LongWritable, ProductWritable, LongWritable, Text> {
        private LongWritable outputKey;
        private Text outputValue;
        @Override
        protected void setup(
                Mapper<LongWritable, ProductWritable, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            this.outputKey = new LongWritable();
            this.outputValue = new Text();
        }

        @Override
        protected void map(LongWritable key, ProductWritable value,
                Mapper<LongWritable, ProductWritable, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            outputKey.set(key.get());
            outputValue.set(value.toString());
            context.write(outputKey, outputValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //在创建Configuration对象时紧跟着配置当前作业需要使用的JDBC配置
        DBConfiguration.configureDB(
                conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.88.10:3306/shops",
                "root",
                "123456");

        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(ReadDBApp.class);

        job.setInputFormatClass(DBInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setMapperClass(ReadDBAppMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        //配置当前作业要查询的SQL语句和接收查询结果的JavaBean
        DBInputFormat.setInput(
                job,
                ProductWritable.class,
                "SELECT `id`,`name`,`model`,`color`,`price` FROM `product`",
                "SELECT COUNT(1) FROM `product`");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int createJob(String[] args) {
        Configuration conf = new Configuration();
        // 客户端Socket写入DataNode的超时时间（以毫秒为单位）
        conf.setLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 7200000);
        int status = 0;
        try {
            status = ToolRunner.run(conf, new ReadDBApp(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static void main(String[] args) {
        if (args.length!=1) {
            System.out.println("Usage: "+ JOB_NAME+" Input parameters <OUTPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }

    }

}