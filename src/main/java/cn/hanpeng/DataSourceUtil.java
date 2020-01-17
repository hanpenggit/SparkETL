package cn.hanpeng;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * @author hanpeng
 * @create 2019-12-29 9:53
 */
@Slf4j
public class DataSourceUtil {

    public static DruidDataSource getSourceDataSource(TaskVo task){
        return dateSource(task.getSourceUrl(),task.getSourceUser(),task.getSourcePwd(),task.getSourceDriver());
    }
    public static DruidDataSource getTargetDataSource(TaskVo task){
        return dateSource(task.getTargetUrl(),task.getTargetUser(),task.getTargetPwd(),task.getTargetDriver());
    }
    private static String filters;
    private static Integer maxActive;
    private static Integer maxWait;
    private static Integer minIdle;
    private static Integer timeBetweenEvictionRunsMillis;
    private static Integer minEvictableIdleTimeMillis;
    private static String validationQuery;
    private static boolean testWhileIdle;
    private static boolean testOnBorrow;
    private static boolean testOnReturn;
    private static boolean poolPreparedStatements;
    private static Integer maxPoolPreparedStatementPerConnectionSize;
    private static Integer initialSize;

    public static void load(Properties properties){
        filters=properties.getProperty("spring.datasource.druid.filters");
        maxActive= Integer.valueOf(properties.getProperty("spring.datasource.druid.max-active"));
        maxWait= Integer.valueOf(properties.getProperty("spring.datasource.druid.max-wait"));
        minIdle= Integer.valueOf(properties.getProperty("spring.datasource.druid.min-idle"));
        timeBetweenEvictionRunsMillis= Integer.valueOf(properties.getProperty("spring.datasource.druid.time-between-eviction-runs-millis"));
        minEvictableIdleTimeMillis= Integer.valueOf(properties.getProperty("spring.datasource.druid.min-evictable-idle-time-millis"));
        validationQuery=properties.getProperty("spring.datasource.druid.validation-query");
        testWhileIdle= Boolean.parseBoolean(properties.getProperty("spring.datasource.druid.test-while-idle"));
        testOnBorrow= Boolean.parseBoolean(properties.getProperty("spring.datasource.druid.test-on-borrow"));
        testOnReturn= Boolean.parseBoolean(properties.getProperty("spring.datasource.druid.test-on-return"));
        poolPreparedStatements= Boolean.parseBoolean(properties.getProperty("spring.datasource.druid.pool-prepared-statements"));
        maxPoolPreparedStatementPerConnectionSize= Integer.valueOf(properties.getProperty("spring.datasource.druid.max-open-prepared-statements"));
        initialSize= Integer.valueOf(properties.getProperty("spring.datasource.druid.initialSize"));
    }

    @SneakyThrows
    public static DruidDataSource dateSource(String url, String user, String pwd, String driver){
        // 1.创建连接池对象
        DruidDataSource ds = new DruidDataSource();
        // 2.设置连接数据库的账号密码
        ds.setDriverClassName(driver);
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(pwd);
        // 最大连接数
        ds.setMaxActive(maxActive);
        //初始化连接数数量
        ds.setInitialSize(initialSize);
        //最大等待时间
        ds.setMaxWait(maxWait);
        ds.setPoolPreparedStatements(poolPreparedStatements);
        ds.setMaxPoolPreparedStatementPerConnectionSize(maxPoolPreparedStatementPerConnectionSize);
        ds.setMinIdle(minIdle);
        ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        ds.setValidationQuery(validationQuery);
        ds.setTestWhileIdle(testWhileIdle);
        ds.setTestOnBorrow(testOnBorrow);
        ds.setTestOnReturn(testOnReturn);
        ds.setFilters(filters);
        return ds;
    }
}
