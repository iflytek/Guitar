package com.iflytek.guitar.export.error;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangli
 * 请按如下方式定义异常表：<br>
 * CREATE TABLE `ExceptionTable` (<br>
 *   `id` int(11) NOT NULL AUTO_INCREMENT,<br>
 *   `user` varchar(64) DEFAULT NULL COMMENT '入库用户',<br>
 *   `modelname` varchar(128) DEFAULT NULL COMMENT '入库模块名称',<br>
 *   `type` varchar(128) DEFAULT NULL COMMENT '入库数据的数据类型',<br>
 *   `context` text  COMMENT '出现错误时的上下文信息',<br>
 *   `errorinfo` text  COMMENT '出错原因提示',<br>
 *   `timestamp` int(11) DEFAULT NULL COMMENT '入库数据的时间戳',<br>
 *   PRIMARY KEY (`id`),<br>
 * 	 KEY `table_index` (`user`,`modelname`,`type`,`timestamp`)<br>
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;<br>
 */
public class ExceptionDbLogWriter {
	private LogWriter logWriter = null;
	private final static String LOG_TABLE = "ExceptionTable";
	private Connection conn = null; // should be static and read from proterties file
	
	// column(field) names
	private final static String COL_USER = "user";
	private final static String COL_MODEL = "modelname";
	private final static String COL_TYPE = "type";
	private final static String COL_CONTEXT = "context";
	private final static String COL_ERRORINFO = "errorinfo";
	private final static String COL_TIMESTAMP = "timestamp";
	
	private TableImportInfo tii = null;
	
	public ExceptionDbLogWriter(Connection conn) throws SQLException {
		this.conn = conn;
		logWriter = new LogWriter(this.conn, LOG_TABLE, LogWriter.WRITE_TO_DB);
		tii = logWriter.getDbTableInfo();
	}
	
	public ExceptionDbLogWriter(String db, Connection conn) throws SQLException {
		this.conn = conn;
		logWriter = new LogWriter(this.conn, db+"."+ LOG_TABLE, LogWriter.WRITE_TO_DB);
		tii = logWriter.getDbTableInfo();
	}
	
	public void write(String type, String context, String errorinfo, String modelName, int timestamp){
		Map<String, Object> logRow = new HashMap<String, Object>();
		String user;
		Integer maxLen_user = tii.getLenMax(COL_USER);
		Integer maxLen_model = tii.getLenMax(COL_MODEL);
		Integer maxLen_type = tii.getLenMax(COL_TYPE);
		Integer maxLen_context = tii.getLenMax(COL_CONTEXT);
		Integer maxLen_errorinfo = tii.getLenMax(COL_ERRORINFO);
		
		assert(maxLen_user!=null && maxLen_model!=null && maxLen_type!=null && maxLen_context!=null && maxLen_errorinfo!=null);
		try {
			user = UserGroupInformation.getCurrentUser().getUserName();
		} catch (IOException e) {
			user = System.getProperty("user.name");
		}
		user = ToDbUtils.getSubStringByte(user,maxLen_user);
		modelName = (null==modelName)?"default":ToDbUtils.getSubStringByte(modelName, maxLen_model);
		type = ToDbUtils.getSubStringByte(type,maxLen_type);
		context = ToDbUtils.getSubStringByte(context,maxLen_context);
		errorinfo = ToDbUtils.getSubStringByte(errorinfo,maxLen_errorinfo);
		
		logRow.put(COL_USER, user);
		logRow.put(COL_MODEL, modelName);
		logRow.put(COL_TYPE, type);
		logRow.put(COL_CONTEXT, context);
		logRow.put(COL_ERRORINFO, errorinfo);
		logRow.put(COL_TIMESTAMP, timestamp);
		
		logWriter.writeLog(logRow);
	}

}
