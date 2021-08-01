package com.atguigu.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean toCamel) {

        //创建结果集
        ArrayList<T> list = new ArrayList<>();

        try {
            //编译SQL
            PreparedStatement preparedStatement = connection.prepareStatement(querySql);

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
        }

        //返回结果
        return list;
    }

    public static void main(String[] args) {

        System.out.println(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "TM_NAME"));

    }

}
