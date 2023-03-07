package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PhoenixUtil {
  public static void executeSql(String sql){

      Connection conn = null;
      PreparedStatement ps = null;
      try {
          //获取连接
          conn = DruidDSUtil.getConnection();
          //获取数据库操作对象
          ps = conn.prepareStatement(sql);
          //执行SQL语句
          ps.execute();
      } catch (Exception e) {
          e.printStackTrace();
      }finally {
          //释放资源
          if(ps != null){
              try {
                  ps.close();
              } catch (SQLException e) {
                  e.printStackTrace();
              }
          }
          if(conn != null){
              try {
                  conn.close();
              } catch (SQLException e) {
                  e.printStackTrace();
              }
          }
      }
  }
  public static <T>List<T> queryList(String sql , Class<T> clz){
      Connection connection = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      ArrayList<T> list = new ArrayList<>();
      try {
          //获取与数据库的连接
          connection = DruidDSUtil.getConnection();
          //创建sql执行对象
          ps = connection.prepareStatement(sql);
          //执行sql
          rs = ps.executeQuery();
          //处理结果集
          while (rs.next()){
              //创建对象用于封装结果集
              T obj = clz.newInstance();
              ResultSetMetaData metaData = rs.getMetaData();
              for (int i = 1; i <= metaData.getColumnCount(); i++) {
                  String columnName = metaData.getColumnName(i);
                  Object columnValue = rs.getObject(i);
                  BeanUtils.setProperty(obj,columnName,columnValue);
              }
              list.add(obj);
          }
      } catch (Exception throwables) {
          throwables.printStackTrace();
      }finally {
          //关闭连接
          if (rs != null){
              try {
                  rs.close();
              } catch (SQLException throwables) {
                  throwables.printStackTrace();
              }
          }
          if (ps != null){
              try {
                  ps.close();
              } catch (SQLException throwables) {
                  throwables.printStackTrace();
              }
          }
          if (connection != null){
              try {
                  connection.close();
              } catch (SQLException throwables) {
                  throwables.printStackTrace();
              }
          }
      }
       return list;
  }

    public static void main(String[] args) {
        List<JSONObject> list = queryList("select * from GMALL0905_REALTIME.DIM_BASE_PROVINCE", JSONObject.class);
        System.out.println(list);
    }
}


