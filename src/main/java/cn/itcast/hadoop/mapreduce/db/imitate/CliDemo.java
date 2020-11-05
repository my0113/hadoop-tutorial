package cn.itcast.hadoop.mapreduce.db.imitate;

import org.apache.commons.cli.*;

/**
 *
 * @ClassName CliDemo
 * @Description
 * @Created by MengYao
 * @Date 2020/10/31 20:59
 * @Version V1.0
 */
public class CliDemo {

    public static void main(String[] args) {
        // 传入正确的命令（指定程序要使用的配置文件）
        args = new String[] { "-c", "config.xml" };
        testOptions(args);
    }

    public static void testOptions(String[] args) {
        Options options = new Options();
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        CommandLineParser parser = new PosixParser();
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp("testApp", options, true);
            }
            // 打印opts的名称和值
            Option[] opts = commandLine.getOptions();
            if (opts != null) {
                for (Option opt1 : opts) {
                    String name = opt1.getLongOpt();
                    String value = commandLine.getOptionValue(name);
                    System.out.println(name + "=>" + value);
                }
            }
        }
        catch (ParseException e) {
            hf.printHelp("testApp", options, true);
        }
    }

}
