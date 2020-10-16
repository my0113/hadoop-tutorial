package cn.itcast.hadoop.mapreduce.db.imitate.config;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @ClassName ImportConfig
 * @Description
 * @Created by MengYao
 * @Date 2020/10/14 16:58
 * @Version V1.0
 */
public class CommandConfig {

    private Map<String, Object> cmd = new HashMap<String, Object>();

    // 仅支持MySQL
    public static final String DRIVER_CLASS = "driver";
    // 指定导入模式
    public static final String IMPORT = "import";
    // 指定导出模式
    public static final String EXPORT = "export";
    // 指定MySQL访问用户
    public static final String USERNAME = "username";
    // 指定MySQL连接URL
    public static final String CONNECT = "connect";
    // 指定MySQL访问用户的密码
    public static final String PASSWORD = "password";
    // 指定表名称
    public static final String TABLE = "table";
    // 指定表字段
    public static final String FIELDS = "fields";
    // 指定导出路径
    public static final String EXPORT_DIR = "exportDir";
    // 指定导入路径
    public static final String TARGET_DIR = "targetDir";
    // 指定分隔符
    public static final String FIELD_TERMINATED_BY = "fieldsTerminatedBy";
    //指定map任务数量
    public static final String MAP = "map";

    private CommandConfig() {
        this.cmd.put(DRIVER_CLASS, "com.mysql.jdbc.Driver");
        this.cmd.put(MAP, 1);
    }

    public static CommandConfig create() {
        return new CommandConfig();
    }

    public void setImport(boolean flag) {
        this.cmd.put(IMPORT, true);
        this.cmd.put(EXPORT, false);
    }

    public boolean isImport() {
        return Boolean.parseBoolean(this.cmd.get(IMPORT).toString());
    }

    public void setExport(boolean flag) {
        this.cmd.put(EXPORT, true);
        this.cmd.put(IMPORT, false);
    }

    public boolean isExport() {
        return Boolean.parseBoolean(this.cmd.get(EXPORT).toString());
    }

    public void setUsername(String username) {
        this.cmd.put(USERNAME, username);
    }

    public String getUsername() {
        return this.cmd.get(USERNAME).toString();
    }

    public String getDriverClass() {
        return cmd.get(DRIVER_CLASS).toString();
    }

    public void setConnect(String connect) {
        this.cmd.put(CONNECT, connect);
    }

    public String getConnect() {
        return this.cmd.get(CONNECT).toString();
    }

    public void setPassword(String passowrd) {
        this.cmd.put(PASSWORD, passowrd);
    }

    public String getPassword() {
        return this.cmd.get(PASSWORD).toString();
    }

    public void setTable(String table) {
        this.cmd.put(TABLE, table);
    }

    public String getTable() {
        return this.cmd.get(TABLE).toString();
    }

    public void setFields(String fields) {
        this.cmd.put(FIELDS, fields.split(","));
    }

    public String[] getFields() {
        return (String[])this.cmd.get(FIELDS);
    }

    public void setExportDir(String exportDir) {
        this.cmd.put(EXPORT_DIR, exportDir);
    }

    public String getExportDir() {
        return this.cmd.get(EXPORT_DIR).toString();
    }

    public void setTargetDir(String targetDir) {
        this.cmd.put(TARGET_DIR, targetDir);
    }

    public String getTargetDir() {
        return this.cmd.get(TARGET_DIR).toString();
    }

    public void setFieldTerminatedBy(String fieldTerminatedBy) {
        this.cmd.put(FIELD_TERMINATED_BY, fieldTerminatedBy);
    }

    public String getFieldTerminatedBy() {
        return this.cmd.get(FIELD_TERMINATED_BY).toString();
    }

    public void setMap(int map) {
        this.cmd.put(MAP, map);
    }

    public int getMap() {
        return Integer.parseInt(this.cmd.get(MAP).toString());
    }

    @Override
    public String toString() {
        return cmd.toString();
    }

}
