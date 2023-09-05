//package com.anryg.bigdata;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import io.netty.util.internal.StringUtil;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;
///***
// * Redis数据库的连接池操作类
// * @author linsh
// *
// */
//public class RedisUtil {
//
//    //日志控制器
//    protected static Log logger = LogFactory.getLog(RedisUtil.class);
//    /**从配置文件中读取配置信息**/
//    //Redis服务器端口号
//    private static int Port = FileUtil.getPropertyValueInt("Redis.properties", "PORT");
//    //Redis服务器端Ip地址组（多个redis数据中心的Ip，备用）
//    private static String Server_address_array = FileUtil.getPropertyValueString("Redis.properties", "SERVER_ARRAY");
//    //Redis服务器端访问密码
//    private static String Auth = FileUtil.getPropertyValueString("Redis.properties", "AUTH");
//    //Redis连接超时时长（单位：毫秒）
//    private static int TimeOut = FileUtil.getPropertyValueInt("Redis.properties", "TIMEOUT");
//    //可用连接实例的最大数目，默认值为8，如果赋值为-1，则表示不限制；如果pool已经分配了MAX_ACTIVE个jedis实例，则此时pool的状态为exhausted(耗尽)。
//    private static int MAX_ACTIVE = FileUtil.getPropertyValueInt("Redis.properties", "MAX_ACTIVE");
//    //一个Pool最多有多少个状态为idle(空闲)的jedis实例，默认值为8个
//    private static int MAX_IDLE = FileUtil.getPropertyValueInt("Redis.properties", "MAX_IDLE");
//    //待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException异常
//    private static int MAX_WAIT = FileUtil.getPropertyValueInt("Redis.properties", "MAX_WAIT");
//    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的
//    private static boolean TEST_ON_BORROW = FileUtil.getPropertyValueBool("Redis.properties", "TEST_ON_BORROW");
//    /**
//     * redis数据有效时长
//     */
//    public final static int EXRP_HOUR = 60*60;          //1小时
//    public final static int EXRP_DAY = 60*60*24;        //1天
//    public final static int EXRP_MOUTH = 60*60*24*30;   //1个月
//    //Jedis连接池
//    private static JedisPool jedisPool = null;
//    /**
//     * 初始化连接池
//     */
//    private static void InitPoolConfig() {
//        //假如第一个ip的redis访问异常，则选用第二个
//        try {
//            JedisPoolConfig config = new JedisPoolConfig();
//            config.setMaxTotal(MAX_ACTIVE);
//            config.setMaxIdle(MAX_IDLE);
//            config.setMaxWaitMillis((long)MAX_WAIT);
//            config.setTestOnBorrow(TEST_ON_BORROW);
//            jedisPool = new JedisPool(config, Server_address_array.split(",")[0],Port,TimeOut);
//        } catch (Exception e) {
//            logger.error("first redis server fail:" +e);
//            try {
//                JedisPoolConfig config = new JedisPoolConfig();
//                config.setMaxTotal(MAX_ACTIVE);
//                config.setMaxIdle(MAX_IDLE);
//                config.setMaxWaitMillis((long)MAX_WAIT);
//                config.setTestOnBorrow(TEST_ON_BORROW);
//                jedisPool = new JedisPool(config, Server_address_array.split(",")[1],Port,TimeOut);
//            } catch (Exception e2) {
//                logger.error("add redis servers fail:" +e);
//            }
//        }
//    }
//    /**
//     * 在多线程环境下，避免重复初始化
//     */
//    private static synchronized void PoolInit() {
//        if(jedisPool == null){
//            InitPoolConfig();
//        }
//    }
//    /**
//     * 同步获取Jedis实例
//     * @return
//     */
//    public synchronized static Jedis getJedis() {
//        if(jedisPool == null){
//            InitPoolConfig();
//        }
//        Jedis jedis = null;
//        try {
//            if(jedisPool!=null){
//                jedis = jedisPool.getResource();
//            }
//        } catch (Exception e) {
//            logger.error("Get redis fail:"+ e);
//        }finally {
//            returnResource(jedis);
//        }
//        return jedis;
//    }
//    /**
//     * 释放Jedis资源
//     * @param jedis
//     */
//    private static void returnResource(Jedis jedis) {
//        if(jedis!=null&&jedisPool!=null){
//            jedisPool.returnBrokenResource(jedis);
//        }
//    }
//    /**
//     * 设置 String
//     * @param key
//     * @param value
//     */
//    public static void setString(String key ,String value){
//        try {
//            value = StringUtil.isNullOrEmpty(value) ? "" : value;
//            getJedis().set(key,value);
//        } catch (Exception e) {
//            logger.error("Set key error : " e);
//        }
//    }
//    /**
//     * 设置 过期时间
//     * @param key
//     * @param seconds 以秒为单位
//     * @param value
//     */
//    public static void setString(String key ,int seconds,String value){
//        try {
//            value = StringUtil.isNullOrEmpty(value) ? "" : value;
//            getJedis().setex(key, seconds, value);
//        } catch (Exception e) {
//            logger.error("Set keyex error : " e);
//        }
//    }
//    /**
//     * 获取String值
//     * @param key
//     * @return value
//     */
//    public static String getString(String key){
//        if(getJedis() == null || !getJedis().exists(key)){
//            return null;
//        }
//        return getJedis().get(key);
//    }
//}