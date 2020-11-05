package cn.itcast.hadoop.mapreduce.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ProductBean implements Writable, DBWritable {

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