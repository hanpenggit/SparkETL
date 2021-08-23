package cn.hanpeng;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 工具类
 *
 * @author hanpeng
 * @create 2019-08-26 15:06
 */
@Slf4j
public class StringUtil {
    private static final Pattern pattern = Pattern.compile ("(\\[[^\\]]*\\])");;
    /**
     * 用来替换${xxx}的正则
     */
    private static final Pattern p = Pattern.compile("(\\$\\{)([\\w]+)(\\})");

    @SneakyThrows
    public static TaskVo check_args(String [] args) {
        TaskVo.TaskVoBuilder taskVoBuilder = TaskVo.builder();
        CommandLineParser parser = new DefaultParser();
        Options opts = new Options();
        Option opt_h = Option.builder("h").longOpt("help").numberOfArgs(0).required(false).desc("help").build();
        Option opt_l = Option.builder("l").longOpt("isLocal").numberOfArgs(1).required(false).type(String.class).desc("isLocal,default is local").build();
        Option opt_n = Option.builder("n").longOpt("name").numberOfArgs(1).required(false).type(String.class).desc("spark.app.name, required").build();
        Option opt_p = Option.builder("p").longOpt("parallelism").numberOfArgs(1).required(false).type(String.class).desc("parallelism,default is 1").build();
        Option opt_e = Option.builder("e").longOpt("executorMemory").numberOfArgs(1).required(false).type(String.class).desc("spark.executor.memory,default is 1g").build();
        Option opt_k = Option.builder("k").longOpt("start").numberOfArgs(1).required(false).type(String.class).desc("start, required").build();
        Option opt_j = Option.builder("j").longOpt("end").numberOfArgs(1).required(false).type(String.class).desc("end, required").build();
        Option opt_f = Option.builder("f").longOpt("format").numberOfArgs(1).required(false).type(String.class).desc("format").build();
        Option opt_r = Option.builder("r").longOpt("readLog").numberOfArgs(1).required(false).type(String.class).desc("readLog, default is false").build();
        Option opt_g = Option.builder("g").longOpt("repartitionNum").numberOfArgs(1).required(false).type(String.class).desc("repartitionNum, The number of partitions to be re-partitioned, which should not be less than the number of parallel partitions").build();
        Option opt_bs = Option.builder("bs").longOpt("batchSize").numberOfArgs(1).required(false).type(String.class).desc("batchSize").build();
        Option opt_fs = Option.builder("fs").longOpt("fetchSize").numberOfArgs(1).required(false).type(String.class).desc("fetchSize").build();
        Option opt_it = Option.builder("it").longOpt("interval").numberOfArgs(1).required(false).type(String.class).desc("interval").build();
        Option opt_ss = Option.builder("ss").longOpt("selectSql").numberOfArgs(1).required(false).type(String.class).desc("selectSql").build();
        Option opt_c = Option.builder("c").longOpt("columnCount").numberOfArgs(1).required(false).type(String.class).desc("columnCount").build();
        Option opt_is = Option.builder("is").longOpt("insertSql").numberOfArgs(1).required(false).type(String.class).desc("insertSql").build();
        Option opt_config = Option.builder("config").longOpt("config").numberOfArgs(1).required(false).type(String.class).desc("config path").build();
        Option opt_sp = Option.builder("sp").longOpt("partitions").numberOfArgs(1).required(false).type(String.class).desc("Partition name, multiple separated by commas").build();
        opts.addOption(opt_h);
        opts.addOption(opt_l);
        opts.addOption(opt_n);
        opts.addOption(opt_p);
        opts.addOption(opt_e);
        opts.addOption(opt_k);
        opts.addOption(opt_j);
        opts.addOption(opt_r);
        opts.addOption(opt_g);
        opts.addOption(opt_f);
        opts.addOption(opt_bs);
        opts.addOption(opt_fs);
        opts.addOption(opt_it);
        opts.addOption(opt_ss);
        opts.addOption(opt_c);
        opts.addOption(opt_is);
        opts.addOption(opt_config);
        opts.addOption(opt_sp);
        CommandLine line = parser.parse(opts, args);
        String startTime="";
        String endTime="";
        if (args == null || args.length == 0 || line.hasOption("h")) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("encrypt", opts);
            System.exit(0);
        }
        if(line.hasOption("n")){
            String n=line.getOptionValue("n");
            taskVoBuilder.name(n);
        }else{
            log.error(opt_n.getDescription());
            System.exit(0);
        }
        int timeV=0;
        if(line.hasOption("k")){
            startTime=line.getOptionValue("k");
            if(StringUtils.isNotBlank(startTime)){
                timeV++;
                taskVoBuilder.start(startTime);
            }
        }
        if(line.hasOption("j")){
            endTime=line.getOptionValue("j");
            if(StringUtils.isNotBlank(endTime)){
                timeV++;
                taskVoBuilder.end(endTime);
            }
        }
        boolean partitionExist=false;
        if(line.hasOption("sp")){
            String partitions=line.getOptionValue("sp");
            if(StringUtils.isNotBlank(partitions)){
                partitionExist=true;
                taskVoBuilder.partitions(partitions);
            }
        }
        if(timeV==0){
            if(!partitionExist){
                log.error("startTime and endTime cannot both be empty");
                System.exit(0);
            }
        }else if(timeV==2){
            if(startTime.compareTo(endTime)>=0){
                log.error("The startTime should not be greater than or equal to the endTime.");
                System.exit(0);
            }
        }
        if(line.hasOption("f")){
            String format=line.getOptionValue("f");
            taskVoBuilder.format(format);
        }

        if(line.hasOption("l")){
            String l=line.getOptionValue("l");
            taskVoBuilder.isLocal(Boolean.parseBoolean(l));
        }
        if(line.hasOption("r")){
            String r=line.getOptionValue("r");
            taskVoBuilder.readLog(Boolean.parseBoolean(r));
        }
        int parallelism=1;
        if(line.hasOption("p")){
            String p=line.getOptionValue("p");
            parallelism=Integer.parseInt(p);
            taskVoBuilder.parallelism(parallelism);
        }
        if(line.hasOption("g")){
            String g=line.getOptionValue("g");
            int g_int=Integer.parseInt(g);
            taskVoBuilder.repartitionNum(g_int);
            if(g_int<0){
                log.error("repartitionNum Not less than zero");
                System.exit(0);
            }else if(g_int<parallelism){
                log.error("The number of partitions to be re-partitioned, which should not be less than the number of parallel partitions");
                System.exit(0);
            }

        }
        if(line.hasOption("e")){
            String e=line.getOptionValue("e");
            taskVoBuilder.executorMemory(e);
        }
        String config=null;
        if(line.hasOption("config")){
            config=line.getOptionValue("config");
        }
        InputStream is;
        if(StringUtils.isNotBlank(config)){
            is = new FileInputStream(config);
        }else{
            is = StringUtil.class.getClassLoader().getResourceAsStream("config.properties");
        }
        Properties properties=new Properties();
        properties.load(is);
        DataSourceUtil.load(properties);
        taskVoBuilder.sourceUser(properties.getProperty("source_user"));
        taskVoBuilder.sourcePwd(properties.getProperty("source_pwd"));
        taskVoBuilder.sourceUrl(properties.getProperty("source_url"));
        taskVoBuilder.sourceDriver(properties.getProperty("source_driver"));
        taskVoBuilder.targetUser(properties.getProperty("target_user"));
        taskVoBuilder.targetPwd(properties.getProperty("target_pwd"));
        taskVoBuilder.targetUrl(properties.getProperty("target_url"));
        taskVoBuilder.targetDriver(properties.getProperty("target_driver"));
        taskVoBuilder.checkSql(properties.getProperty("checkSql"));
        String insertSql=properties.getProperty("insertSql");
        taskVoBuilder.insertSql(insertSql);
        String selectSql = properties.getProperty("selectSql");
        taskVoBuilder.selectSql(selectSql);
        if (properties.containsKey("columnCount")) {
            int selectCount=Integer.parseInt(properties.getProperty("columnCount"));
            taskVoBuilder.selectCount(selectCount);
        }
        if (properties.containsKey("batchSize")) {
            int batchSize=Integer.parseInt(properties.getProperty("batchSize","1000"));
            taskVoBuilder.batchSize(batchSize);
        }
        if (properties.containsKey("fetchSize")) {
            int fetchSize=Integer.parseInt(properties.getProperty("fetchSize","1000"));
            taskVoBuilder.fetchSize(fetchSize);
        }
        if (properties.containsKey("intervalTime")) {
            int intervalTime=Integer.parseInt(properties.getProperty("intervalTime"));
            taskVoBuilder.interval(intervalTime);
        }

        if(line.hasOption("bs")){
            String bs=line.getOptionValue("bs");
            taskVoBuilder.batchSize(Integer.parseInt(bs));
        }
        if(line.hasOption("fs")){
            String fs=line.getOptionValue("fs");
            taskVoBuilder.fetchSize(Integer.parseInt(fs));
        }
        if(line.hasOption("it")){
            String it=line.getOptionValue("it");
            taskVoBuilder.interval(Integer.parseInt(it));
        }
        if(line.hasOption("ss")){
            String ss=line.getOptionValue("ss");
            taskVoBuilder.selectSql(ss);
        }
        if(line.hasOption("c")){
            String c=line.getOptionValue("c");
            taskVoBuilder.selectCount(Integer.parseInt(c));
        }
        if(line.hasOption("is")){
            String v=line.getOptionValue("is");
            taskVoBuilder.insertSql(v);
        }

        TaskVo task = taskVoBuilder.build();
        log.info(task.toString());
        return task;
    }

    @Deprecated
    public static Set<String> readTaskLog() throws IOException {
        //读取log4j的日志文件，需要与log4j中配置文件的路径相同
        String pathname = "logs/1.log";
        File filename = new File(pathname);
        FileInputStream inputStream = new FileInputStream(filename);
        InputStreamReader reader = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(reader);
        String line = "";
        line = br.readLine();
        Set<String> taskTimeList=new HashSet<>();
        while (line != null) {
            Matcher matcher = pattern.matcher (line);
            while (matcher.find ()){
                String group = matcher.group();
                if(group.length()!=31){
                    continue;
                }
                group=group.replace("[","").replace("]","");
                taskTimeList.add(group);
            }
            line = br.readLine();
        }
        br.close();
        reader.close();
        inputStream.close();
        return taskTimeList;
    }

    public static String parse(String content, Map<String,String> kvs){
        Matcher m = p.matcher(content);
        StringBuffer sr = new StringBuffer();
        while(m.find()){
            String group = m.group();
            String value=kvs.get(group);
            if(StringUtils.isNotBlank(value)){
                m.appendReplacement(sr, kvs.get(group));
            }
        }
        m.appendTail(sr);
        return sr.toString();
    }

}
