package com.anryg.bigdata;

import java.util.Arrays;

public class IPClient {
    public static void main(String[] args) throws Exception {
        IPUtils.ipCountryImport("C:\\Users\\duthu\\Downloads\\ip.merge.txt",15);
        String[] split = IpSearch.getAddrByIP(RedisClientUtils.getSingleRedisClient(), "11.0.8.1").split("-");
//       System.out.println(Arrays.toString(split));
    }
}
