package cn.itcast.hadoop.mapreduce.db.imitate;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 通用Writable，以代替特定的JavaPOJO
 * @ClassName GeneralWritable
 * @Description
 * @Created by MengYao
 * @Date 2020/10/14 18:48
 * @Version V1.0
 */
public class GeneralWritable implements Writable, DBWritable {

    private Configuration conf;
    private String[] fields;
    private final Map<String, Object> data = new LinkedHashMap<>(50);


    public GeneralWritable(Configuration conf) {
        this.conf = conf;
        Preconditions.checkNotNull(conf, "Configuration对象不得为空");
        this.fields = conf.getStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        data.forEach((k,v) -> {
                try {
                    if (v instanceof Boolean) {out.writeBoolean((boolean)v);}
                    if (v instanceof Byte) {out.writeByte((byte)v);}
                    if (v instanceof Short) {out.writeShort((short)v);}
                    if (v instanceof Integer) {out.writeInt((int)v);}
                    if (v instanceof Long) {out.writeLong((long)v);}
                    if (v instanceof Float) {out.writeFloat((float)v);}
                    if (v instanceof Double) {out.writeDouble((double)v);}
                    if (v instanceof String) { out.writeUTF(v.toString()); }
                } catch (IOException e) {
                    e.printStackTrace();
                }
        });
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        for (int i = 1; i <= fields.length; i++) {
            String fieldName = fields[i - 1];
            if (data.containsKey(fieldName)) {
                Object colValue = data.get(fieldName);
                if (colValue.getClass().getName().equals(String.class.getName())||colValue.getClass().getName().equals(Object.class.getName())) { ps.setString(i, data.get(fieldName).toString()); }
                if (colValue.getClass().getName().equals(Boolean.class.getName())) { ps.setBoolean(i, (boolean)data.get(fieldName)); }
                if (colValue.getClass().getName().equals(Integer.class.getName())) { ps.setInt(i, (int)data.get(fieldName)); }
                if (colValue.getClass().getName().equals(BigInteger.class.getName())||colValue.getClass().getName().equals(Long.class.getName())) { ps.setLong(i, (long)data.get(fieldName)); }
                if (colValue.getClass().getName().equals(BigDecimal.class.getName())) { ps.setBigDecimal(i, ((BigDecimal)data.get(fieldName))); }
                if (colValue.getClass().getName().equals(Float.class.getName())) { ps.setFloat(i, (float)data.get(fieldName)); }
                if (colValue.getClass().getName().equals(Double.class.getName())) { ps.setDouble(i, (double)data.get(fieldName)); }
                if (colValue.getClass().getName().equals(Date.class.getName())) { ps.setDate(i, new java.sql.Date(((Date)data.get(fieldName)).getTime())); }
                if (colValue.getClass().getName().equals(Short.class.getName())) { ps.setShort(i, (short)data.get(fieldName)); }
                if (colValue.getClass().getName().equals(Time.class.getName())) { ps.setTime(i, new java.sql.Time((long)data.get(fieldName))); }
                if (colValue.getClass().getName().equals(Timestamp.class.getName())) { ps.setTimestamp(i, new java.sql.Timestamp((long)data.get(fieldName))); }
            }
        }
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i <= fields.length; i++) {
            String fieldName = fields[i - 1];
            String colType = metaData.getColumnClassName(i);
            if (colType.equals(String.class.getName())) { data.put(fieldName, rs.getString(i)); }
            if (colType.equals(Boolean.class.getName())) { data.put(fieldName, rs.getBoolean(i)); }
            if (colType.equals(Integer.class.getName())) { data.put(fieldName, rs.getInt(i)); }
            if (colType.equals(BigInteger.class.getName())||colType.equals(Long.class.getName())) { data.put(fieldName, rs.getLong(i)); }
            if (colType.equals(BigDecimal.class.getName())) { data.put(fieldName, rs.getBigDecimal(i)); }
            if (colType.equals(Float.class.getName())) { data.put(fieldName, rs.getFloat(i)); }
            if (colType.equals(Double.class.getName())) { data.put(fieldName, rs.getDouble(i)); }
            if (colType.equals(Date.class.getName())) { data.put(fieldName, new Date(rs.getDate(i).getTime())); }
            if (colType.equals(Short.class.getName())) { data.put(fieldName, rs.getShort(i)); }
            if (colType.equals(Time.class.getName())) { data.put(fieldName, rs.getTime(i).getTime()); }
            if (colType.equals(Timestamp.class.getName())) { data.put(fieldName, rs.getTimestamp(i).getTime()); }
            if (colType.equals(Object.class.getName())) { data.put(fieldName, rs.getString(i)); }
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(32);
        for (String field : fields) {
            builder.append(data.get(field)).append(",");
        }
        builder.setLength(builder.length()-1);
        return builder.toString();
    }
}
