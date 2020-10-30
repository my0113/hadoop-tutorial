package cn.itcast.hadoop.mapreduce.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName MyHashPartitioner
 * @Description
 * @Created by MengYao
 * @Date 2020/10/28 17:55
 * @Version V1.0
 */
public class DFSTool {

    public static List<String> readText(Configuration conf, String dir) {
        List<String> result = new ArrayList<>();
        FileSystem fs = null;
        FSDataInputStream in = null;
        BufferedReader bin = null;
        try {
            fs = FileSystem.get(conf);
            Path remotePath = new Path(dir);
            in = fs.open(remotePath);
            bin = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = bin.readLine()) != null) {
                result.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bin.close();
                in.close();
//                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://node1.itcast.cn:9820");
        List<String> list = readText(conf, "/apps/data2/pay.csv");
        list.forEach(System.out::println);

    }

}
