package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean toCamel) {

        //创建结果集
        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = null;

        try {
            //编译SQL
            preparedStatement = connection.prepareStatement(querySql);

            //执行查询
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            //遍历查询结果集,封装对象放入list集合
            while (resultSet.next()) {

                //创建泛型对象
                T t = clz.newInstance();

                for (int i = 1; i < columnCount + 1; i++) {
                    //取出列名
                    String columnName = metaData.getColumnName(i);
                    if (toCamel) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }

                    //取出值
                    String value = resultSet.getString(i);

                    //给对象赋值
                    BeanUtils.setProperty(t, columnName, value);
                }

                //将对象添加至集合
                list.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        //返回结果
        return list;
    }

    public static void main(String[] args) throws Exception {

//        System.out.println(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "TM_NAME"));
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        System.out.println(queryList(connection,
                "select * from GMALL210108_REALTIME.DIM_BASE_TRADEMARK where id='10'",
                JSONObject.class,
                false));

        connection.close();

    }

}
