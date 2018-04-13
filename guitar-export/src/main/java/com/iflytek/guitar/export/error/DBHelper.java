package com.iflytek.guitar.export.error;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBHelper {
	
	private String url;
	private String userName;
	private String password;

	private Connection conn = null;
	
	/*
	 public DBHelper(String url, String userName, String passwd) {
		this.url = url;
		this.userName = userName;
		this.password = passwd;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			conn = DriverManager.getConnection(this.url, this.userName, password);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	*/

	
	public DBHelper(Connection conn) {
		this.conn = conn;
	}
	
	public ResultSet execQuery (String sql) throws SQLException{
		
		ResultSet rs = null;
		try{
			synchronized (conn) {
				// Need I to close Statement after use? 
				Statement stm = conn.createStatement();
				rs =  stm.executeQuery(sql);
			}
		}finally{

		}
		return rs;
	}
	
	public int execUpdate(String sql) throws SQLException{
		
		int ret;
		try{
			synchronized (conn) {
				Statement stm = conn.createStatement();
				ret = stm.executeUpdate(sql);
				conn.commit();
			}
		}finally{
			
		}
		return ret;
	}
	
	public void closeConn() throws SQLException{
		synchronized (conn) {
			if (conn != null && !conn.isClosed()){
				conn.close();
			}
		}
	}
}
