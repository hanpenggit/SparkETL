package cn.hanpeng;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashSet;
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
public class StringUtil {
    private static Pattern pattern = Pattern.compile ("(\\[[^\\]]*\\])");;
    private static Logger logger = Logger.getLogger(StringUtil.class);

    public static TaskVo check_args(String [] args) throws ParseException, IOException {
        TaskVo.TaskVoBuilder taskVoBuilder = TaskVo.builder();
        CommandLineParser parser = new DefaultParser();
        Options opts = new Options();
        Option opt_h = Option.builder("h").longOpt("help").numberOfArgs(0).required(false).desc("help").build();
        Option opt_l = Option.builder("l").longOpt("isLocal").numberOfArgs(1).required(false).type(String.class).desc("isLocal,default is local").build();
        Option opt_n = Option.builder("n").longOpt("name").numberOfArgs(1).required(false).type(String.class).desc("spark.app.name, required").build();
        Option opt_p = Option.builder("p").longOpt("parallelism").numberOfArgs(1).required(false).type(String.class).desc("parallelism,default is 1").build();
        Option opt_e = Option.builder("e").longOpt("executorMemory").numberOfArgs(1).required(false).type(String.class).desc("spark.executor.memory,default is 1g").build();
        Option opt_k = Option.builder("k").longOpt("startTime").numberOfArgs(1).required(false).type(String.class).desc("startTime, required").build();
        Option opt_j = Option.builder("j").longOpt("endTime").numberOfArgs(1).required(false).type(String.class).desc("endTime, required").build();
        Option opt_r = Option.builder("r").longOpt("readLog").numberOfArgs(1).required(false).type(String.class).desc("readLog, default is true").build();
        Option opt_g = Option.builder("g").longOpt("repartitionNum").numberOfArgs(1).required(false).type(String.class).desc("repartitionNum, The number of partitions to be re-partitioned, which should not be less than the number of parallel partitions").build();
        opts.addOption(opt_h);
        opts.addOption(opt_l);
        opts.addOption(opt_n);
        opts.addOption(opt_p);
        opts.addOption(opt_e);
        opts.addOption(opt_k);
        opts.addOption(opt_j);
        opts.addOption(opt_r);
        opts.addOption(opt_g);
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
            logger.error(opt_n.getDescription());
            System.exit(0);
        }
        if(line.hasOption("k")){
            startTime=line.getOptionValue("k");
            taskVoBuilder.startTime(startTime);
        }else{
            logger.error(opt_k.getDescription());
            System.exit(0);
        }
        if(line.hasOption("j")){
            endTime=line.getOptionValue("j");
            taskVoBuilder.endTime(endTime);
        }else{
            logger.error(opt_j.getDescription());
            System.exit(0);
        }
        if(startTime.compareTo(endTime)>=0){
            logger.error("The startTime should not be greater than or equal to the endTime.");
            System.exit(0);
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
                logger.error("repartitionNum Not less than zero");
                System.exit(0);
            }else if(g_int<parallelism){
                logger.error("The number of partitions to be re-partitioned, which should not be less than the number of parallel partitions");
                System.exit(0);
            }

        }
        if(line.hasOption("e")){
            String e=line.getOptionValue("e");
            taskVoBuilder.executorMemory(e);
        }
        InputStream is = StringUtil.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties=new Properties();
        properties.load(is);
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
        int selectCount=Integer.parseInt(properties.getProperty("columnCount"));
        taskVoBuilder.selectCount(selectCount);
        int batchSize=Integer.parseInt(properties.getProperty("batchSize","1000"));
        taskVoBuilder.batchSize(batchSize);
        int fetchSize=Integer.parseInt(properties.getProperty("fetchSize",batchSize+""));
        taskVoBuilder.fetchSize(fetchSize);
        int intervalTime=Integer.parseInt(properties.getProperty("intervalTime"));
        taskVoBuilder.intervalTime(intervalTime);
        TaskVo task = taskVoBuilder.build();
        logger.info(task.toString());
        return task;
    }

    public static Set<String> readTaskLog() throws IOException {
        //读取log4j的日志文件，需要与log4j中配置文件的路径相同
        String pathname = "logs/1.log";
        File filename = new File(pathname);
        FileInputStream inputStream = new FileInputStream(filename);
        InputStreamReader reader = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(reader);
        String line = "";
        line = br.readLine();
        StringBuilder sr=new StringBuilder();
        while (line != null) {
            line = br.readLine();
            sr.append(line);
        }
        br.close();
        reader.close();
        inputStream.close();
        Matcher matcher = pattern.matcher (sr.toString());
        Set<String> taskTimeList=new HashSet<>();
        while (matcher.find ()){
            String group = matcher.group();
            if(group.length()!=31){
                continue;
            }
            group=group.replace("[","").replace("]","");
            taskTimeList.add(group);
        }
        return taskTimeList;
    }
}
