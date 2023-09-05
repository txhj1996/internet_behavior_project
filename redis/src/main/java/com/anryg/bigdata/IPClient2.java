package com.anryg.bigdata;

import java.util.Arrays;

public class IPClient2 {
    public static void main(String[] args) throws Exception {
        IPUtils.ipCountryImport("C:\\Users\\duthu\\Downloads\\ip.merge.txt",15);
//        String[] split = IpSearch.getAddrByIP(RedisClientUtils.getSingleRedisClient(), "1.215.254.255").split("-");
//       System.out.println(Arrays.toString(split));
    }
}
