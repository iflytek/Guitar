package com.iflytek.guitar.share.db;

import com.iflytek.guitar.share.utils.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBConnect {
    private static final Log LOG = LogFactory.getLog(DBConnect.class);

    private static String DB_URL_FORMAT_2_DB = "jdbc:%s://%s:%d/%s?characterEncoding=utf-8&rewriteBatchedStatements=true&noAccessToProcedureBodies=true&Pooling=false&autoReconnect=true&maxReconnects=3&initialTimeout=6&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    private static String DB_URL_FORMAT_NOT_DB = "jdbc:%s://%s:%d/?characterEncoding=utf-8&rewriteBatchedStatements=true&noAccessToProcedureBodies=true&Pooling=false&autoReconnect=true&maxReconnects=3&initialTimeout=6&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

    private String driver = "";
    private String ip = "";
    private Long port = 0L;
    private String dbName = "";
    private String userName = "";
    private String passwd = "";
    private Connection conn = null;
    private PreparedStatement statModel = null;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("dbConnInfo [driver=");
        builder.append(driver);
        builder.append(", ip=");
        builder.append(ip);
        builder.append(", port=");
        builder.append(port);
        builder.append(", userName=");
        builder.append(userName);
        builder.append(", passwd=");
        builder.append(passwd);
        builder.append(", dbName=");
        builder.append(dbName);
        builder.append("]");
        return builder.toString();
    }

    public DBConnect(String driver, String ip, Long port, String dbName, String userName, String passwd) {
        this.driver = driver;
        this.ip = ip;
        this.port = port;
        this.dbName = dbName;
        this.userName = userName;
        this.passwd = passwd;
    }

    public class SelectWalker {
        private ResultSet rltSet = null;
        ResultSetMetaData rsmd = null;
        private int columnCount = -1;

        public SelectWalker(ResultSet rltSet) throws SQLException {
            this.rltSet = rltSet;

            /* 获取列数 */
            rsmd = rltSet.getMetaData();
            columnCount = rsmd.getColumnCount();
        }

        public boolean next() throws SQLException {
            return rltSet.next();
        }

        public Map<String, Object> getRecord() throws SQLException {
            /* 按行读取字段, 保存到一个Map中  */
            Map<String, Object> mapFields = Maps.newHashMap();
            for (int idx = 1; idx <= columnCount; idx++) {
                String columnName = rsmd.getColumnName(idx);
                Object columnValue = rltSet.getObject(idx);
                mapFields.put(columnName, columnValue);
            }

            return mapFields;
        }
    }

    public static class SelectResult {
        private Map<Map<String, String>, Map<String, Object>> mapRecord = Maps.newHashMap();

        public SelectResult() {

        }

        public Iterator<Map.Entry<Map<String, String>, Map<String, Object>>> getIterator() {
            return mapRecord.entrySet().iterator();
        }

        protected void addRecord(Map<String, String> dimFields, Map<String, Object> tarFields) {
            mapRecord.put(dimFields, tarFields);
        }

        public boolean isKeyExist(String... keyInfo) {
            if (null == keyInfo || keyInfo.length <= 0 || (0 != keyInfo.length % 2)) {
                return false;
            }

            Map<String, String> dimFields = Maps.newTreeMap();
            for (int idx = 1; idx <= keyInfo.length; idx += 2) {
                dimFields.put(keyInfo[idx - 1], keyInfo[idx]);
            }

            return mapRecord.containsKey(dimFields);
        }

        /* 参数形式为:
         * keyName1 value1 keyName2 valuee2...
         * 一个表有n个维度, 那么这里的参数个数应该是8个 */
        public Map<String, Object> getRecordByKey(String... keyInfo) {
            if (null == keyInfo || keyInfo.length <= 0 || (0 != keyInfo.length % 2)) {
                return null;
            }

            Map<String, String> dimFields = Maps.newTreeMap();
            for (int idx = 1; idx <= keyInfo.length; idx += 2) {
                dimFields.put(keyInfo[idx - 1], keyInfo[idx]);
            }

            return mapRecord.get(dimFields);
        }

        /* 参数形式为:
         * keyName1 value1 keyName2 valuee2...
         * 一个表有n个维度, 那么这里的参数个数应该是8个 */
        public Object getFieldByKey(String fieldName, String... keyInfo) {
            Map<String, Object> mapValue = Maps.newHashMap();
            if (null == fieldName || null == keyInfo || keyInfo.length <= 0 || (0 != keyInfo.length % 2)) {
                return null;
            }

            Map<String, String> dimFields = Maps.newTreeMap();
            for (int idx = 1; idx <= keyInfo.length; idx += 2) {
                dimFields.put(keyInfo[idx - 1], keyInfo[idx]);
            }

            mapValue = mapRecord.get(dimFields);
            if (null == mapValue) {
                return null;
            }

            return mapValue.get(fieldName);
        }

        /* dimFields一定要是一个TreeMap */
        public Object getFieldByKey(String fieldName, Map<String, String> dimFields) {
            Map<String, Object> mapValue = Maps.newHashMap();
            mapValue = mapRecord.get(dimFields);
            if (null == mapValue) {
                return null;
            }

            return mapValue.get(fieldName);
        }
    }

    public Connection getConnection() {
        if (isConnOk()) {
            return conn;
        }

        String connUrl = null;
        if (null == this.dbName || this.dbName.length() <= 0) {
            connUrl = String.format(DB_URL_FORMAT_NOT_DB, this.driver, this.ip, this.port);
        } else {
            connUrl = String.format(DB_URL_FORMAT_2_DB, this.driver, this.ip, this.port, this.dbName);
        }

        try {
            LOG.info(this);
            Class.forName("com.mysql.jdbc.Driver");
            LOG.info(connUrl);
            conn = DriverManager.getConnection(connUrl, this.userName, this.passwd);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            conn = null;
            e.printStackTrace();
        }

        return conn;
    }

    public boolean isConnOk() {
        if (null == conn) {
            return false;
        }

        try {
            if (conn.isClosed()) {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public String getDropSql(String tableName) {
        return "drop table if exists " + tableName;
    }

    public int exceSingleResultSql(String sql) throws SQLException {
        Statement exceSql = conn.createStatement();
        exceSql.setQueryTimeout(60);

        int iRet = -1;
        boolean bRet = exceSql.execute(sql);
        if (false == bRet) {
            iRet = exceSql.getUpdateCount();
        }
        conn.commit();
        exceSql.close();
        return iRet;
    }

    public int exceUnSafeSingleResultSql(String sql) throws SQLException {
        Statement exceSql = conn.createStatement();
        exceSql.setQueryTimeout(60);

        int iRet = -1;
        boolean bRet = exceSql.execute(sql);
        if (false == bRet) {
            iRet = exceSql.getUpdateCount();
        }

        exceSql.close();
        return iRet;
    }

    public int exceSafeBatch(List<String> lstSql) {
        if (null == lstSql || lstSql.size() <= 0) {
            return Constants.RET_INT_ERROR;
        }

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            for (String tmpSql : lstSql) {
                stmt.addBatch(tmpSql);
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
            if (null != stmt) {
                try {
                    stmt.close();
                } catch (SQLException eClose) {
                    eClose.printStackTrace();
                }
            }

            return Constants.RET_INT_ERROR;
        }

        try {
            stmt.executeBatch();

            /* 执行成功, 立即commit生效 */
            conn.commit();
        } catch (SQLException e2) {
            e2.printStackTrace();

            /* 执行不成功, 则回滚 */
            try {
                conn.rollback();
            } catch (SQLException eRollback) {
                eRollback.printStackTrace();
            }

            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public int exceUnSafeBatch(List<String> lstSql) {
        if (null == lstSql || lstSql.size() <= 0) {
            return Constants.RET_INT_ERROR;
        }

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            for (String tmpSql : lstSql) {
                stmt.addBatch(tmpSql);
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
            if (null != stmt) {
                try {
                    stmt.close();
                } catch (SQLException eClose) {
                    eClose.printStackTrace();
                }
            }

            return Constants.RET_INT_ERROR;
        }

        try {
            stmt.executeBatch();
        } catch (SQLException e2) {
            e2.printStackTrace();

            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public int exceQueryCountSql(String sql) throws SQLException {
        Statement exceSql = conn.createStatement();
        exceSql.setQueryTimeout(60);

        int iRet = -1;
        ResultSet rltSet = exceSql.executeQuery(sql);
        if (null == rltSet) {
            return -1;
        }
        conn.commit();

        if (rltSet.next()) {
            iRet = rltSet.getInt(1);
        } else {
            /* 如果 */
            iRet = -1;
        }

        exceSql.close();
        return iRet;
    }

    public ResultSet exceQuerySelectSql(String sql) throws SQLException {
        Statement exceSql = conn.createStatement();
        exceSql.setQueryTimeout(60);

        return exceSql.executeQuery(sql);
    }

    public boolean createInsertModel(String sqlModel) {
        boolean bRet = true;
        try {
            statModel = conn.prepareStatement(sqlModel);
        } catch (SQLException e) {
            e.printStackTrace();
            statModel = null;
            bRet = false;
        }

        return bRet;
    }

    public boolean setFeildValue(List<String> lstInsertFeilds) {
        int idx = 1;
        for (String value : lstInsertFeilds) {
            try {
                statModel.setString(idx, value);
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }

            idx++;
        }

        try {
            statModel.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public int exceModel() {
        int iRet = Constants.RET_INT_ERROR;
        if (null == statModel) {
            return iRet;
        }

        try {
            iRet = 0;
            int[] iRetArray = statModel.executeBatch();
            for (int idx = 0; idx < iRetArray.length; idx++) {
                if (iRetArray[idx] >= 0) {
                    iRet++;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return Constants.RET_INT_ERROR;
        }

        return iRet;
    }

    public int clearModel() {
        int iRet = Constants.RET_INT_ERROR;
        if (null == statModel) {
            return iRet;
        }

        try {
            statModel.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public void closeModel() {
        if (null == statModel) {
            return;
        }

        try {
            statModel.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        statModel = null;
    }

    public void setAutoCommit(boolean auto) {
        try {
            conn.setAutoCommit(auto);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("set auto commit");
        }
    }

    public int commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public int rollback() {
        try {
            conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public void closeConn() {
        try {
            if (null != conn) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public SelectWalker selectRowWalker(String sql) throws Exception {
        /* 从合并后的表中读取一行行记录 */
        ResultSet rltSet = exceQuerySelectSql(sql);
        if (null == rltSet) {
            return null;
        }

        return new SelectWalker(rltSet);
    }

    public SelectResult selectRowSet(String sql, Set<String> setDimFields) throws Exception {
        /* 从合并后的表中读取一行行记录 */
        ResultSet rltSet = exceQuerySelectSql(sql);
        if (null == rltSet) {
            return null;
        }

        SelectResult selectResult = new SelectResult();
        SelectWalker walker = new SelectWalker(rltSet);
        while (walker.next()) {
            Map<String, Object> mapFields = walker.getRecord();

            Map<String, String> dimFields = Maps.newTreeMap();
            Iterator<String> itSet = setDimFields.iterator();
            while (itSet.hasNext()) {
                String dimFieldName = itSet.next();
                dimFields.put(dimFieldName, mapFields.remove(dimFieldName).toString());
            }

            selectResult.addRecord(dimFields, mapFields);
        }

        return selectResult;
    }

    public int InsertRow(String tableName, Map<String, Object> mapFields) throws Exception {
        if (null == tableName || tableName.length() <= 0) {
            LOG.error("InsertRow: tableName param error");
            return Constants.RET_INT_ERROR;
        }

        if (null == mapFields || mapFields.size() <= 0) {
            LOG.error("InsertRow: mapFields param error");
            return Constants.RET_INT_ERROR;
        }

        /* INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....) */
        boolean firstFlag = true;
        String strFields = "";
        String strValues = "";
        for (Map.Entry<String, Object> entry : mapFields.entrySet()) {
            if (firstFlag) {
                firstFlag = false;
            } else {
                strFields += ", ";
                strValues += ", ";
            }

            strFields += entry.getKey();
            strValues += "'" + entry.getValue().toString() + "'";
        }

        String insertSql = "insert into " + tableName + " (" + strFields + ") values (" + strValues + ")";
        if (exceUnSafeSingleResultSql(insertSql) <= 0) {
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public int UpdateRow(String tableName, Map<String, Object> dimMap, Map<String, Object> tarMap) throws Exception {
        if (null == tableName || tableName.length() <= 0) {
            LOG.error("UpdateRow: tableName param error");
            return Constants.RET_INT_ERROR;
        }

        if (null == dimMap || dimMap.size() <= 0) {
            LOG.error("UpdateRow: dimMap param error");
            return Constants.RET_INT_ERROR;
        }

        if (null == tarMap || tarMap.size() <= 0) {
            LOG.error("UpdateRow: tarMap param error");
            return Constants.RET_INT_ERROR;
        }

        boolean firstFlag = true;
        String updateSql = "update " + tableName + " set ";
        for (Map.Entry<String, Object> entry : tarMap.entrySet()) {
            if (firstFlag) {
                firstFlag = false;
            } else {
                updateSql += ", ";
            }

            updateSql += entry.getKey() + "='" + entry.getValue().toString() + "'";
        }

        updateSql += " where ";

        firstFlag = true;
        for (Map.Entry<String, Object> entry : dimMap.entrySet()) {
            if (firstFlag) {
                firstFlag = false;
            } else {
                updateSql += " and ";
            }

            updateSql += entry.getKey() + "='" + entry.getValue().toString() + "'";
        }

        if (exceUnSafeSingleResultSql(updateSql) <= 0) {
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public int DeleteRow(String tableName, Map<String, Object> mapFields) throws Exception {
        if (null == tableName || tableName.length() <= 0) {
            LOG.error("DeleteRow: tableName param error");
            return Constants.RET_INT_ERROR;
        }

        if (null == mapFields || mapFields.size() <= 0) {
            LOG.error("DeleteRow: mapFields param error");
            return Constants.RET_INT_ERROR;
        }

        boolean firstFlag = true;
        String conditions = "";
        for (Map.Entry<String, Object> entry : mapFields.entrySet()) {
            if (firstFlag) {
                firstFlag = false;
            } else {
                conditions += " and ";
            }

            conditions += entry.getKey() + "=" + "'" + entry.getValue().toString() + "'";
        }

        String delSql = "delete from " + tableName + " where " + conditions;
        if (exceUnSafeSingleResultSql(delSql) <= 0) {
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

}
