package com.examine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DB {
    // 初始化
    public Connection init() {

        // 不同的数据库有不同的驱动
        String driverName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://bj-cdb-1ejkcvn0.sql.tencentcdb.com:62732/guide_sound?useUnicode=true&characterEncoding=UTF-8";
        String user = "root";
        String password = "guide_sound_123";
        Connection conn = null;
        try {

            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功..");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
