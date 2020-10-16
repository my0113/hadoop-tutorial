package cn.itcast.hadoop.mapreduce.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 读取普通文件输出到数据库
 * @ClassName WriteDBApp
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 15:31
 * @Version V1.0
 */
public class WriteDBApp extends Configured implements Tool {

    // 作业名称
    private static final String JOB_NAME = WriteDBApp.class.getSimpleName();

    /**
     * 这个JavaBean需要实现Hadoop的序列化接口Writable和与数据库交互时的序列化接口DBWritable
     * 官方源码定义如下：
     *         public class DBOutputFormat<K  extends DBWritable, V> extends OutputFormat<K,V>
     *         也就是说DBOutputFormat要求输出的key必须是继承自DBWritable的类型
     * @author mengyao
     *
     */
    static class ProductWritable implements Writable, DBWritable {

        private long id;            // bigint(20) NOT NULL AUTO_INCREMENT,
        private String name;        // varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '商品名称',
        private String model;        // varchar(30) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '型号',
        private String color;        // varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '颜色',
        private double price;        // decimal(10,0) DEFAULT NULL COMMENT '售价'

        public void set(long id, String name, String model,
                        String color, double price) {
            this.id = id;
            this.name = name;
            this.model = model;
            this.color = color;
            this.price = price;
        }

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
        public void write(DataOutput out) throws IOException {
            out.writeLong(id);
            out.writeUTF(name);
            out.writeUTF(model);
            out.writeUTF(color);
            out.writeDouble(price);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = in.readLong();
            this.name = in.readUTF();
            this.model = in.readUTF();
            this.color = in.readUTF();
            this.price = in.readDouble();

        }

    }

    static class WriteDBAppMapper extends Mapper<LongWritable, Text, NullWritable, ProductWritable> {

        private NullWritable outputKey;
        private ProductWritable outputValue;

        @Override
        protected void setup(
                Mapper<LongWritable, Text, NullWritable, ProductWritable>.Context context)
                throws IOException, InterruptedException {
            this.outputKey = NullWritable.get();
            this.outputValue = new ProductWritable();
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, NullWritable, ProductWritable>.Context context)
                throws IOException, InterruptedException {
            //插入数据库成功的计数器
            final Counter successCounter = context.getCounter("exec", "successfully");
            //插入数据库失败的计数器
            final Counter faildCounter = context.getCounter("exec", "faild");
            //获取DFS上已有的机构化数据
            final String[] fields = value.toString().split("\t");
            //DBOutputFormatApp这个MapReduce应用导出的数据包含long类型的key，所以忽略key从1开始
            if (fields.length > 5) {
                long id = Long.parseLong(fields[1]);
                String name = fields[2];
                String model = fields[3];
                String color = fields[4];
                double price = Double.parseDouble(fields[5]);
                this.outputValue.set(id, name, model, color, price);
                context.write(outputKey, outputValue);
                //如果插入数据库成功则递增1，表示成功计数
                successCounter.increment(1L);
            } else {
                //如果插入数据库失败则递增1，表示失败计数
                faildCounter.increment(1L);
            }
        }
    }

    /**
     * DBOutputFormatReducer
     *         输入的key即DBOutputFormatMapper输出的key
     *         输出的value即DBOutputFormatMapper输出的value
     *         输出的key必须是继承自DBWritable的类型（DBOutputFormat要求输出的key必须是继承自DBWritable的类型）
     *         输出的value无所谓，这里我使用的是NullWritable作为占位输出
     * @author mengyao
     *
     */
    static class DBOutputFormatReducer extends Reducer<NullWritable, ProductWritable, ProductWritable, NullWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<ProductWritable> value,
                              Reducer<NullWritable, ProductWritable, ProductWritable, NullWritable>.Context context)
                throws IOException, InterruptedException {
            for (ProductWritable productWritable : value) {
                context.write(productWritable, key);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //在创建Configuration对象时紧跟着配置当前作业需要使用的JDBC配置
        DBConfiguration.configureDB(
                conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.104:3306/test?useUnicode=true&characterEncoding=utf8",
                "root",
                "123456");
        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(WriteDBApp.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);

        job.setMapperClass(WriteDBAppMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(ProductWritable.class);

        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);

        job.setReducerClass(DBOutputFormatReducer.class);
        job.setOutputKeyClass(ProductWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        //配置当前作业输出到数据库的表、字段信息
        DBOutputFormat.setOutput(job, "product", new String[]{"id", "name", "model", "color", "price"});

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int createJob(String[] args) {
        Configuration conf = new Configuration();
        conf.set("dfs.datanode.socket.write.timeout", "7200000");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "268435456");
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
            System.out.println("Usage: "+JOB_NAME+" Input parameters <INPUT_PATH>");
        } else {
            int status = createJob(args);
            System.exit(status);
        }
    }
}
