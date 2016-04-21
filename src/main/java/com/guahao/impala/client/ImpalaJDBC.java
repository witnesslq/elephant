package com.guahao.impala.client;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.ResultSet;  
import java.sql.SQLException;  
import java.sql.Statement;  
import java.util.ArrayList;
import java.util.List;

public class ImpalaJDBC {  
    private static final String IMPALAD_HOST = "192.168.1.90";  
    private static final String IMPALAD_JDBC_PORT = "21050";
    private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";  
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";  
    private Connection conn;
    
    public ImpalaJDBC() {
    	try {
    		Class.forName(JDBC_DRIVER_NAME);
			this.conn = DriverManager.getConnection(CONNECTION_URL);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    public List<String> getResultList(String sql) {
    	Statement stmt;
    	ResultSet rs;
    	List<String> l = new ArrayList<String>();
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				l.add(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
        return l;
    } 
}  
