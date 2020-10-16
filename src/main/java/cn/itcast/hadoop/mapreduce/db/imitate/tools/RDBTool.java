package cn.itcast.hadoop.mapreduce.db.imitate.tools;

import java.sql.*;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 数据库操作工具类
 * @ClassName RDBTool
 * @Description
 * @Created by MengYao
 * @Date 2020/10/14 18:34
 * @Version V1.0
 */
public class RDBTool {

    /**
     *
     * @param driver
     * @param url
     * @param user
     * @param password
     * @param table
     * @return
     */
    public static Properties metadata(String driver, String url, String user, String password, String table) {
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url , user, password);
            return RDBTool.metadata(connection, table, (conn, tbl) -> {
                PreparedStatement ps = null;
                ResultSet rs = null;
                Properties prop = new Properties();
                try {
                    ps = conn.prepareStatement(tbl);
                    rs = ps.executeQuery();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        String columnTypeName = metaData.getColumnTypeName(i);
                        String columnClassName = metaData.getColumnClassName(i);
                        prop.setProperty(columnName, columnClassName + "," + columnTypeName);
                    }
                } catch (Exception e) {
                } finally {
                    try {
                        if (!ps.isClosed()) {ps.close();}
                        if (!rs.isClosed()) {rs.close();}
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
                return prop;
            });
        } catch (ClassNotFoundException|SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (!connection.isClosed()) {connection.close();}
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    
    /**
     * 查询指定表的metadata信息
     * @param connection    连接对象
     * @param table         表将会被处理为：SELECT * FROM <TABLE> WHERE 1=0
     * @param func          获取metadata的自定义实现
     * @param <T>           返回metadata
     * @return
     */
    public static <T> T metadata(Connection connection, String table, BiFunction<Connection, String, T> func) {
        if (Objects.isNull(table)||Objects.isNull(connection)) {
            return null;
        }
        if (Objects.nonNull(table)) {
            table = "SELECT * FROM tbl WHERE 1=0".replace("tbl", table);
        }
        return func.apply(connection, table);
    }

    /**
     * 无条件查询
     * @param <T>
     * @param sql
     * @param func
     * @return
     */
    public static <T> List<T> query(String sql, Function<String, List<T>> func) {
        List<T> result = func.apply(sql);
        return result;
    }

    /**
     * 单条件查询
     * @param <T>
     * @param sql
     * @param params
     * @param func
     * @return
     */
    public static <T> T query(String sql, T params, BiFunction<String, T, T> func) {
        T result = null;
        try {
            result = func.apply(sql, params);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 多条件查询
     * @param <T>
     * @param sql
     * @param params
     * @param func
     * @return
     */
    public static <T> List<T> query(String sql, List<T> params, BiFunction<String, List<T>, List<T>> func) {
        List<T> result = null;
        try {
            result = func.apply(sql, params);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 单条数据写入
     * @param <T>
     * @param sql
     * @param func
     */
    public static <T> void save(String sql, Consumer<String> func) {
        try {
            func.accept(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条数据写入
     * @param <T>
     * @param sql
     * @param data
     * @param func
     */
    public static <T> void save(String sql, T data, BiConsumer<String, T> func) {
        try {
            func.accept(sql, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量数据写入
     * @param <T>
     * @param sql
     * @param data
     * @param func
     */
    public static <T> void save(String sql, List<T> data, BiConsumer<String, List<T>> func) {
        if (null==data || data.size()<0) {
            System.err.println("==== Parameter data is null! ====");
        } else {
            System.out.println("==== 开始写入数据... ====");
            try {
                func.accept(sql, data);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("==== 写入数据异常! ====");
            }
            System.out.println("==== 数据写入完成. ====");
        }
    }

    /**
     * 单条数据修改
     * @param <T>
     * @param sql
     * @param data
     * @param func
     */
    public static <T> void update(String sql, T data, BiConsumer<String, T> func) {
        if (null==data) {
            System.err.println("==== Parameter data is null! ====");
        } else {
            try {
                func.accept(sql, data);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("==== 写入数据异常! ====");
            }
        }
    }

    /**
     * 批量数据修改
     * @param <T>
     * @param sql
     * @param data
     * @param func
     */
    public static <T> void update(String sql, List<T> data, BiConsumer<String, List<T>> func) {
        try {
            func.accept(sql, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据ID删除
     * @param sql
     * @param id
     */
    public static void deleteById(String sql, long id, BiConsumer<String, Long> func) {
        try {
            func.accept(sql, id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行写好的删除语句
     * @param sql
     * @param func
     */
    public static void delete(String sql, Consumer<String> func) {
        try {
            func.accept(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
