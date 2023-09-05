package com.anryg.bigdata;

import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Set;

public class IPClient1 {
    public static void main(String[] args) throws Exception {
//        IPUtils.ipCountryImport("C:\\Users\\duthu\\Downloads\\ip.merge.txt",15);
        Jedis jredis = RedisClientUtils.getSingleRedisClient();
        jredis.select(15);

//        jredis.zadd("zset01", 60d, "v1");
//        jredis.zadd("zset01", 170d, "v2");
//        jredis.zadd("zset01", 180d, "v3");
//        jredis.zadd("zset01", 90d, "v4");
//        Set<String> s1 = jredis.zrange("zset01", 0, -1);
//        System.out.println(s1);

//        Set<String> ipAndAddr = jredis.zrange("ipAndAddr", 0, -1);
//        System.out.println(ipAndAddr);

        Set<String> zrange = jredis.zrange("ipAndAddr",0,20);
        System.out.println(zrange);

        for(String member : zrange){

//            System.out.println(member);
            Double ipAndAddr = jredis.zscore("ipAndAddr", member);
            System.out.println(member+"\t"+ipAndAddr);

        }

        System.out.println("+++++++++++++++++++++");


    }
}
