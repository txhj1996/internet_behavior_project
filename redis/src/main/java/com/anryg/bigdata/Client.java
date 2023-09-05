package com.anryg.bigdata;

import redis.clients.jedis.Jedis;

public class Client {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop102", 6379);
//        Jedis jedis = new Jedis("47.100.114.188", 6379);

        System.out.println(jedis.ping());
    }
}
