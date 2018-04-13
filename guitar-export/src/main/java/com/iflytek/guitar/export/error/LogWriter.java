package com.iflytek.guitar.export.error;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Log includes different types and different level, 
 * type including Exception log, common log, error log and so on, 
 * level including release log, debug log and so on, 
 * We can write log to database or file or both
 *
 */
public class LogWriter {
	private Connection conn = null;
	
	/**
	 * if write log to database, table name is needed
	 */
	private String logTable = null;
	/**
	 * write log to database or not.
	 */
	private boolean writeDbLog = false;
	private boolean writeFileLog = false;
	private TableImportInfo tii = null;
	
	public final static int WRITE_TO_DB = 1;
	public final static int WRITE_TO_FILE = 2;
	
	public LogWriter(Connection conn, String logTable, int writeTo) throws SQLException {
		this.conn = conn;
		this.logTable = logTable;
		if ((writeTo & WRITE_TO_DB) != 0){
			writeDbLog = true;
			assert(this.logTable!=null && conn!=null);
			tii = new TableImportInfo(this.logTable,conn);
		}
		if ((writeTo & WRITE_TO_FILE) != 0){
			writeFileLog = true;
		}
	}
	
	/**
	 * @param log
	 * This method should be static, is that it? 
	 */
	public void writeLog(Map<String, Object> log){
		if (writeFileLog){
			// write to file using log4j
		}
		if (writeDbLog){
			synchronized (conn) {
				try {
					//pst = conn.prepareStatement(tii.getBatchSql());
					//tii.setPst(pst);
					tii.bindRow(log); 
					int rowCount = tii.getPst().executeUpdate();
					if (rowCount == 0){
						System.out.println("Write log to DB error: write 0 row(s)!");
						System.out.println("Data: ");
						System.out.println(log.toString());
					}
				} catch (SQLException e) {
					System.out.println("Write log to DB error:");
					e.printStackTrace();
					System.out.println("Data: ");
					System.out.println(log.toString());
				}
			}
		}
	}
	
	public TableImportInfo getDbTableInfo(){
		return this.tii;
	}

	
}
