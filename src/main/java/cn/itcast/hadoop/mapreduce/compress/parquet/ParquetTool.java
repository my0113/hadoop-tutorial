package cn.itcast.hadoop.mapreduce.compress.parquet;

import cn.itcast.hadoop.mapreduce.bean.OrderBean;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * @ClassName ParquetTool
 * @Description
 * @Created by MengYao
 * @Date 2020/11/12 15:31
 * @Version V1.0
 */
public class ParquetTool {

    /**
     * 字段与类型之间的分隔符：fieldName:fieldType
     */
    public static final String TYPE_SEPERATOR = ":";
    /**
     * 字段与字段之间的分隔符：fieldName,fieldName
     */
    public static final String FIELD_SEPERATOR = ",";


    public static <T> String getSchema(Class<T> caz) {
        return getSchemaAsNameAndType(caz, false);
    }

    public static <T> String getSchemaAsNameAndType(Class<T> caz, boolean containsType) {
        final StringBuilder builder = new StringBuilder(32);
        Field[] fields = caz.getDeclaredFields();
        if (containsType) {
            Stream.of(fields).forEach(field -> {
                builder.append(field.getName()).append(TYPE_SEPERATOR).append(field.getType().getSimpleName()).append(FIELD_SEPERATOR);
            });
        } else {
            Stream.of(fields).forEach(field -> {
                builder.append(field.getName()).append(FIELD_SEPERATOR);
            });
        }
        builder.setLength(builder.length()-1);
        return builder.toString();
    }

    public static void main(String[] args) {
        System.out.println(getSchema(OrderBean.class));
    }

}
