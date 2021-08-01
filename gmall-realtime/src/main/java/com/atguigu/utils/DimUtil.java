package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {

        //查询Redis中的数据
        String redisKey = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        String dimInfo = jedis.get(redisKey);
        if (dimInfo != null) {
            JSONObject dimInfoJson = JSONObject.parseObject(dimInfo);
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return dimInfoJson;
        }

        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id ='" + id + "'";
        System.out.println(querySql);

        //查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection,
                querySql,
                JSONObject.class,
                false);

        //将查询到的数据写入缓存
        JSONObject dimInfoJson = queryList.get(0);
        jedis.set(redisKey, dimInfoJson.toJSONString());
        //设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果数据
        return dimInfoJson;
    }

    public static void delDimInfo(String redisKey) {
        //获取Redis连接
        Jedis jedis = RedisUtil.getJedis();
        //删除数据
        jedis.del(redisKey);
        //归还连接
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);


        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,
                "DIM_BASE_TRADEMARK",
                "10"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,
                "DIM_BASE_TRADEMARK",
                "10"));
        long end2 = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println(end2 - end);

        connection.close();

    }

}
