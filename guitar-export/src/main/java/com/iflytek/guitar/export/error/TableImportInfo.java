package com.iflytek.guitar.export.error;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TableImportInfo {
	
	private PreparedStatement pst = null;
	private String tableName = null;
	private Connection conn = null;
	private Map<String,Pair<SetSqlPara,ParaCheck>> fieldInfo = null;
	private String batchSql = null;
	
	class Pair<T1,T2>{
		public T1 v1;
		public T2 v2;
	};
	
	public TableImportInfo(String tablename, Connection con) throws SQLException {
		super();
		fieldInfo = new HashMap<String, Pair<SetSqlPara,ParaCheck>>();
		tableName = tablename;
		conn = con;
		assert(conn != null);
		analyseTable();
	}

	/**
	 * Attention: this analysis is only suit for mysql database.
	 * @throws SQLException
	 */
	private void analyseTable() throws SQLException {
		if (null == tableName) {
            return;
        }
		DBHelper dbh = new DBHelper(conn);
		ResultSet rs = dbh.execQuery("desc " + tableName);

		if (rs==null){
			throw new SQLException("Can not get info of table '"+tableName+"'");
		}else{
			while(rs.next()){
				// 自增列无需插入数据
				//assert(rs.getString(6)!=null); //
				//if (!rs.getString(6).toLowerCase().equals("auto_increment")){
				String field  = rs.getString(1).toLowerCase();
				String type = rs.getString(2);
				String couldNull = rs.getString(3);
				//String defaultVal = rs.getString(5);
				Pair<SetSqlPara,ParaCheck> pair = getFieldInfo(type,couldNull);
				fieldInfo.put(field, pair);
				//}
			}
		}
	}

	private Pair<SetSqlPara, ParaCheck> getFieldInfo(String type,
			String couldNull) throws SQLException {
		String typeShort;
		int length = -1;
		boolean cldNull = true;
		
		int idxb = type.indexOf('(');
		if (idxb != -1){
			int idxe = type.indexOf(')');
			assert(idxe!=-1 && idxe>idxb+1);
			typeShort = type.substring(0,idxb);
			String len = type.substring(idxb+1, idxe);
			int idxlen = len.indexOf(',');
			if (idxlen != -1){
				length = Integer.parseInt(len.substring(0, idxlen));
			}else{
				length = Integer.parseInt(len);
			}
		}
		else{
			typeShort = type;
		}
		
		if ("YES".equals(couldNull.toUpperCase())){
			cldNull = true;
		}else if ("NO".equals(couldNull.toUpperCase())){
			cldNull = false;
		}else{
			throw new SQLException("Unknown isNull type when analyse table "+tableName +": "+couldNull);
		}
		
		return getFieldInfo(typeShort.toLowerCase(),length,cldNull);
	}

	private Pair<SetSqlPara, ParaCheck> getFieldInfo(String type,
			int length, boolean cldNull) {
		Pair<SetSqlPara, ParaCheck> pair = new Pair<SetSqlPara, ParaCheck>();
		if ("tinyint".equals(type) || "smallint".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
			//pair.v1 = new SetShort();
			pair.v1 = new SetString();
		}else if ("int".equals(type) || "mediumint".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
			//pair.v1 = new SetInt();
			pair.v1 = new SetString();
		}else if("bigint".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
			//pair.v1 = new SetLong();
			pair.v1 = new SetString();
		}else if("varchar".equals(type) || "char".equals(type)){
			pair.v2 = new ParaCheckString(cldNull, length);
			pair.v1 = new SetString();
		}else if("float".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
//			pair.v1 = new SetFloat();
			pair.v1 = new SetString();
		}else if("double".equals(type) || "decimal".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
//			pair.v1 = new SetDouble();
			pair.v1 = new SetString();
		}else if ("tinytext".equals(type)){
			pair.v2 = new ParaCheckString(cldNull, 255);
			pair.v1 = new SetString();
		}else if ("text".equals(type)){
			pair.v2 = new ParaCheckString(cldNull, 65535);
			pair.v1 = new SetString();
		}else if ("mediumtext".equals(type) || "longtext".equals(type)){
			pair.v2 = new ParaCheckString(cldNull, length);
			pair.v1 = new SetString();
		}else if ("date".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
//			pair.v1 = new SetDate();
			pair.v1 = new SetString();
		}else if ("time".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
//			pair.v1 = new SetTime();
			pair.v1 = new SetString();
		}else if("timestamp".equals(type)){
			pair.v2 = new ParaCheckNum(cldNull);
//			pair.v1 = new SetTimestamp();
			pair.v1 = new SetString();
		}else{
			///? need to test
			pair.v2 = new ParaCheckNum(cldNull);
			pair.v1 = new SetObject();
		}

		return pair;
	}


	/**
	 * @return a batch sql like:
	 * "insert into tablename(col1,col2,...) values(?,?,...)";
	 */
	public String getBatchSql(){
		
		if (batchSql != null){
			return batchSql;
		}
		
		StringBuffer sb = new StringBuffer("insert into ");
		sb.append(tableName);
		sb.append(" (");
		Set<String> fields = fieldInfo.keySet(); 
		boolean isFirst = true;
		for(Iterator<String> it = fields.iterator(); it.hasNext();){
			if (!isFirst){
				sb.append(",");
			}else{
				isFirst = false;
			}
			sb.append(it.next());
		}
		
		// mysql 5.5.3 以前的版本utf8默认的只支持3字节编码，而utf8mb4可以支持4位编码。
		sb.append(") values (CAST(? AS CHAR CHARACTER SET utf8mb4)");
		for(int i=1; i<fields.size(); ++i){
			sb.append(",CAST(? AS CHAR CHARACTER SET utf8mb4)");
		}
		sb.append(")");
		batchSql = sb.toString();
		
		System.out.println("batch sql is : " + batchSql);
		return batchSql;
	}

	protected void updateFieldInfo(Map<String, Object> row){
		Map<String,Pair<SetSqlPara,ParaCheck>> fieldInfo_new = new HashMap<String, Pair<SetSqlPara,ParaCheck>>();
		Set<String> fields = fieldInfo.keySet(); 
		for(Iterator<String> it = fields.iterator(); it.hasNext();){
			String fieldName = it.next();
			//Object value = row.get(fieldName);
			if(row.containsKey(fieldName)){
				fieldInfo_new.put(fieldName, fieldInfo.get(fieldName));
			}
		}
		
		fieldInfo = fieldInfo_new;
	}

	public void bindRow(Map<String,Object> row) throws SQLException{
		if (null == batchSql){
			// 根据数据确定要入库的字段，对于not null的字段，如果不入库，则不写ExceptionTable，而直接退出。
			updateFieldInfo(row);
		}
		if (null == pst){
			pst = conn.prepareStatement(getBatchSql());
			pst.setQueryTimeout(100000);
		}
		int i = 1;
		Set<String> fields = fieldInfo.keySet(); 
		for(Iterator<String> it = fields.iterator(); it.hasNext();i++){
			String fieldName = it.next();
			// field name must be case insensitive
			Object value = row.get(fieldName);
			Pair<SetSqlPara,ParaCheck> pair = fieldInfo.get(fieldName);
			if (!pair.v2.check(value)){
				// error, data format error!
				throw new SQLException(fieldName + " value is illegal :"+ value);
			}else{
				try{
					// 该错误分两种，一种类型错误，所有行数据setValue都错误，另一种是部分行数据格式错误。
					// 对于第一种，实际上是类型错误，不应该将这部分异常写入异常表，而应该直接抛异常退出，但目前无法
					// 区分这两种情况。
					pair.v1.setValue(i, value);
				}catch(Exception ex){
					throw new SQLException(fieldName + " value is illegal :"+ value + ". [Detials]:" + ex.getMessage());
				}
			}
		}
		pst.addBatch();
	}
	
	public Integer getLenMax(String field){
		Integer len = null;
		Pair<SetSqlPara, ParaCheck> pair = this.fieldInfo.get(field);	
		if (pair != null){
			len = pair.v2.getLenMax();
		}
		return len;
	}
	
	public PreparedStatement getPst() {
		PreparedStatement pst_ret = pst;
		pst = null;
		return pst_ret;
	}

	public void setPst(PreparedStatement pst) {
		this.pst = pst;
	}
	
	public String getTableName() {
		return tableName;
	}

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	class SetShort implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Short) {
                    pst.setShort(paraIndex,(Short) value);
                } else {
                    pst.setShort(paraIndex, Short.parseShort(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.INTEGER);
			}
		}	
	}
	
	class SetFloat implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Float) {
                    pst.setFloat(paraIndex, (Float) value);
                } else {
                    pst.setFloat(paraIndex, Float.parseFloat(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.FLOAT);
			}
		}	
	}
	
	class SetDouble implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Double) {
                    pst.setDouble(paraIndex, (Double) value);
                } else {
                    pst.setDouble(paraIndex, Double.parseDouble(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.DOUBLE);
			}
		}	
	}
	
	class SetDate implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Date) {
                    pst.setDate(paraIndex, (Date) value);
                } else {
                    pst.setDate(paraIndex, Date.valueOf(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.DATE);
			}
		}	
	}
	
	class SetTime implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Time) {
                    pst.setTime(paraIndex,  (Time) value);
                } else {
                    pst.setTime(paraIndex, Time.valueOf(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.TIME);
			}
		}	
	}
	
	class SetTimestamp implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Timestamp) {
                    pst.setTimestamp(paraIndex,   (Timestamp) value);
                } else {
                    pst.setTimestamp(paraIndex, Timestamp.valueOf(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.TIMESTAMP);
			}
		}	
	}
	
	class SetString implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof String) {
                    pst.setString(paraIndex, (String) value);
                } else {
                    pst.setString(paraIndex, value.toString());
                }
			}else{
				///? 测试表中不是varchar类型是否会出错
				pst.setNull(paraIndex, java.sql.Types.VARCHAR);
			}
		}
	}
	
	class SetInt implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Integer) {
                    pst.setInt(paraIndex, (Integer) value);
                } else {
                    pst.setInt(paraIndex, Integer.parseInt(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.INTEGER);
			}
		}
	}
	
	class SetLong implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				if (value instanceof Long) {
                    pst.setLong(paraIndex, (Long) value);
                } else {
                    pst.setLong(paraIndex, Long.parseLong(value.toString()));
                }
			}else{
				pst.setNull(paraIndex, java.sql.Types.BIGINT);
			}
		}
	}
	
	class SetObject implements SetSqlPara{
		@Override
		public void setValue(int paraIndex, Object value) throws SQLException {
			if (value != null){
				pst.setObject(paraIndex, value);
			}else{
				///? need to test
				pst.setNull(paraIndex, java.sql.Types.OTHER);
			}
		}
	}
	
	class ParaCheckNum implements ParaCheck{
		private boolean cldNull;
		
		public ParaCheckNum(boolean colNull) {
			this.cldNull = colNull;
		}

		@Override
		public boolean check(Object obj) {
			if (!cldNull && obj==null){
				return false;
			}
			return true;
		}

		@Override
		public int getLenMax() {
			return -1;	//数字的长度暂时不检查
		}
	}
	
	class ParaCheckString implements ParaCheck{
		private boolean cldNull;
		private int len = -1;

		public ParaCheckString(boolean cldNull, int len) {
			this.cldNull = cldNull;
			this.len = len;
		}

		@Override
		public boolean check(Object obj) {
			if ((!cldNull && obj==null) || 
					((len!=-1) && (null!=obj) && (obj.toString()).getBytes().length>len)){
				return false;
			}
			return true;
		}
		
		@Override
		public int getLenMax() {
			return len;
		}
		
	}
	
}

