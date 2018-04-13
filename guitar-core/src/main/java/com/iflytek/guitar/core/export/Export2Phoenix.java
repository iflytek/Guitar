package com.iflytek.guitar.core.export;

import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.db.Exportable;
import com.iflytek.guitar.share.avro.io.Pair;
import com.iflytek.guitar.share.avro.mapreduce.FsInput;
import com.iflytek.guitar.share.avro.util.AvroUtils;
import org.apache.avro.file.DataFileReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.Map.Entry;

public class Export2Phoenix {
    private static final Log LOG = LogFactory.getLog(Export2Phoenix.class);
    private static final int DB_COMMIT_TIMES = 10000;
    private static final int DB_EXECUT_MODEL = 1;

    private SimpleDateFormat DEFAULT_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private String ip = "";
    private String url = "";
    private Connection conn = null;

    private PreparedStatement statModel = null;

    public Export2Phoenix(String ip) {
        this.ip = ip;
        this.url = "jdbc:phoenix:" + ip;
    }

    public Connection getConnection() {
        if (ip.length() <= 0 || url.length() <= 0) {
            LOG.error("the Phoenix ip is null,please check it");
            return null;
        }
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(url);
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

    public int exceSingleResultSql(String sql) throws SQLException {
        Statement statement = conn.createStatement();
        int iRet = -1;
        boolean bRet = statement.execute(sql);
        if (false == bRet) {
            iRet = statement.getUpdateCount();
        }
        conn.commit();
        statement.close();
        return iRet;
    }

    protected Map<String, DataFileReader<Object>> getReaders(Path dir, FileSystem fs, Configuration conf)
            throws IOException {
        Path[] aFilePath = FileUtil.stat2Paths(fs.listStatus(dir));
        Arrays.sort(aFilePath);

        Map<String, DataFileReader<Object>> mapReader = Maps.newHashMap();
        for (int i = 0; i < aFilePath.length; i++) {
            /* 判断当前输出目录是mapfile, 还是seqencefile */
            Path readFilePath = aFilePath[i];
            if (fs.isDirectory(readFilePath)) {
                readFilePath = new Path(readFilePath, "data.avro");
            }

            DataFileReader<Object> reader = new DataFileReader<Object>(new FsInput(readFilePath, conf), AvroUtils.getDatumReader(conf));
            if (!mapReader.containsKey(readFilePath.toString())) {
                mapReader.put(readFilePath.toString(), reader);
            } else {
                LOG.error("dir[" + aFilePath[i].toString() + "] has exist.");
                reader.close();
            }
        }

        return mapReader;
    }

    private Map<String, Integer> getTableFiled_type(String tableName) throws SQLException {
        Map<String, Integer> field_type = new TreeMap<String, Integer>();
        Statement statement = conn.createStatement();
        String selectsql = "select * from system.CATALOG where table_name='" + tableName + "'";
        ResultSet rs = statement.executeQuery(selectsql);
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            int datatype = rs.getInt("DATA_TYPE");
            if (columnName == null) {
                continue;
            }
            field_type.put(columnName, datatype);

        }
        return field_type;
    }

    private int prepareInsertModel(String tableName, Map<String, Integer> field_type) {
        StringBuffer fieldModel = new StringBuffer("");
        StringBuffer valueModel = new StringBuffer("");
        ;
        for (String tmpField : field_type.keySet()) {
            if (!"".equals(fieldModel.toString())) {
                fieldModel.append(", ");
            } else {
                fieldModel.append("");
            }

            if (!"".equals(valueModel.toString())) {
                valueModel.append(", ");
            } else {
                valueModel.append("");
            }

            fieldModel.append(tmpField);
            valueModel.append("?");
        }

        String upsertSqlModel = "upsert into \"" + tableName + "\"(" + fieldModel + ") VALUES(" + valueModel + ")";
        try {
            statModel = conn.prepareStatement(upsertSqlModel);
        } catch (SQLException e) {
            e.printStackTrace();
            statModel = null;
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }


    private Map<String, String> getFields(Exportable exportKey,
                                          Exportable exportValue, String timestampName, Date timeValue,
                                          Configuration conf) {

        Map<String, String> mapFields = Maps.newHashMap();
        Map<String, String> tmpKeyMap = exportKey.getExportFeilds();

        if (null == tmpKeyMap) {
            LOG.error("getFields: getExportField from key return null,exportKey is " + exportKey.getClass().getName());
            return null;
        } else {
            for (Entry<String, String> entry : tmpKeyMap.entrySet()) {
                mapFields.put(entry.getKey().toUpperCase(), entry.getValue());
            }
        }

        Map<String, String> tmpValueMap = exportValue.getExportFeilds();
        for (Entry<String, String> entry : tmpValueMap.entrySet()) {
            String tmpKey = (String) entry.getKey();
            String tmpValue = entry.getValue();

            if (mapFields.containsKey(tmpKey)) {
                LOG.error("value=field[" + tmpKey + "] already exist");
                return null;
            } else {
                mapFields.put(tmpKey.toUpperCase(), tmpValue);
            }
        }

		/*加入时间戳字段*/
        String timestampType = conf.get("timestampType");
        if (timestampType != null && "Date".equals(timestampType)) {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String timestampStr = df.format(timeValue);
            mapFields.put(timestampName.toUpperCase(), timestampStr);
        } else {
            String timestampStr = DEFAULT_FORMAT.format(timeValue);
            mapFields.put(timestampName.toUpperCase(), timestampStr);
//			Long timestamp = timeValue.getTime() / 1000;
//			mapFields.put(timestampName.toUpperCase(), timestamp.toString());
        }

        return mapFields;
    }


    private int addInsertSql(Map<String, String> mapFields,
                             Map<String, Integer> field_type, Map<String, String> map) {
        if (mapFields == null || mapFields.size() <= 0) {
            LOG.error("addInsertSql: mapFeilds[" + mapFields.toString() + "] invalid");
            return Constants.RET_INT_ERROR;
        }

        List<String> lstFieldValues = Lists.newArrayList();
        List<Integer> lstFieldTypes = Lists.newArrayList();

        int idx = 1;
        for (Entry<String, Integer> entry : field_type.entrySet()) {
            String field = entry.getKey();
            int type = entry.getValue();
            String fieldvalue = mapFields.get(field);
            if (fieldvalue == null) {
                fieldvalue = " ";
            } else if ("".equals(fieldvalue)) {
                fieldvalue = " ";
            }


            try {
                if (type == 1) {
                    statModel.setString(idx, fieldvalue);
                } else if (type == 6) {
                    statModel.setFloat(idx, Float.valueOf(fieldvalue));
                } else if (type == 8) {
                    statModel.setDouble(idx, Double.valueOf(fieldvalue));
                } else if (type == 4) {
                    statModel.setInt(idx, Integer.valueOf(fieldvalue));
                } else if (type == 12) {
                    statModel.setString(idx, fieldvalue);
                } else if (type == -5) {
                    statModel.setInt(idx, Integer.valueOf(fieldvalue));
                }
                idx++;
            } catch (Exception e) {
                e.printStackTrace();
                return Constants.RET_INT_ERROR;
            }


            // if(fieldvalue == null){
            // lstFieldValues.add("0");
            // }else{
            // lstFieldValues.add(fieldvalue);
            // }
            // lstFieldTypes.add(type);
            //
            // if(lstFieldTypes.size() != lstFieldValues.size() ||
            // lstFieldTypes.size() <= 0 || lstFieldValues.size() <= 0){
            // LOG.error("lstFieldValues.size() != lstFieldTypes.size()");
            // return Constants.RET_INT_ERROR;
            // }
            //
            // for(int i = 0; i <= lstFieldTypes.size();i++){
            // type = lstFieldTypes.get(i);
            // }
        }

        try {
            statModel.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            return Constants.RET_INT_ERROR;
        }
        return Constants.RET_INT_OK;
    }


    private int exceModel() {
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
            LOG.error("executeBatch() fail,rollback()");
            //遇到异常数据进行回滚，因为每10000条数据commit一次，所以一次回滚最多丢失10000条数据。
            try {
                conn.rollback();
                statModel.clearBatch();
            } catch (SQLException e1) {
                LOG.error("rollback(),clearBatch() fail");
            }

            return Constants.RET_INT_ERROR;
        }


        return iRet;
    }

    private void closeModel() {
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

    private int commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            return Constants.RET_INT_ERROR;
        }

        return Constants.RET_INT_OK;
    }

    public int export(String tableName, String inputdir, Date timeValue, String timestampName, Map<String, Map<String, String>> mapAlaisNames, Configuration conf) throws IOException, SQLException {
        if (tableName == null || inputdir == null || timeValue == null) {
            LOG.error("null == tableName || null == inputDir || null == timeValue");
            return Constants.RET_INT_ERROR;
        }

        if (null == timestampName) {
            timestampName = Constants.FIELD_TIMESTAMP_NAME;
        }

        if (!isConnOk()) {
            if (null == getConnection()) {
                LOG.error("conn.getConnection() error.");
                return Constants.RET_INT_ERROR;
            }
        }


		/*读取入库文件 start*/
//        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inputdir);
        if (!fs.exists(inputPath)) {
            LOG.error("inputDir[" + inputPath + "] not exist when tableName is " + tableName);
            return Constants.RET_INT_ERROR;
        }

        Map<String, DataFileReader<Object>> mapReaderInfo = getReaders(inputPath, fs, conf);
        if (mapReaderInfo.size() <= 0) {
            LOG.error("no files to reader when tableName is " + tableName);
            return Constants.RET_INT_ERROR;
        }
        /*读取入库文件 end*/
		
		/*删除表中当前时间戳的数据，重新入库 start*/
//		String delSql = "delete from \"" + tableName + "\" where " + timestampName + "=" + timeValue.getTime() / 1000;
//		int iRet = exceSingleResultSql(delSql);
//		if(Constants.RET_INT_ERROR == iRet){
//			LOG.error("exceSingleResultSql[" + delSql + "] fail.");
//			return Constants.RET_INT_ERROR;
//		}else{
//			LOG.info("exceSingleResultSql[" + delSql + "] success, and update " + iRet + " records.");
//		}
		/*删除表中当前时间戳的数据，重新入库 end*/
		
		/*获取表中的字段及其类型，并建立InsertModel start*/
        Map<String, Integer> field_type = getTableFiled_type(tableName);
        if (null == field_type || field_type.size() <= 0) {
            LOG.error("null == field_type || field_type.size() <= 0 when tableName=" + tableName);
            return Constants.RET_INT_ERROR;
        }

        if (Constants.RET_INT_OK != prepareInsertModel(tableName, field_type)) {
            LOG.error("prepareInsertModel error. when tableName=" + tableName + ", field_type=" + field_type.toString());
            return Constants.RET_INT_ERROR;
        }
		/*获取表中的字段及其类型，并建立InsertModel end*/

        long totalcommitNum = 0L;
        long totalinsertSuccNum = 0L;
        long totalreadNum = 0L;
        for (Entry<String, DataFileReader<Object>> readerInfo : mapReaderInfo.entrySet()) {
            String readFile = readerInfo.getKey();
            DataFileReader<Object> reader = readerInfo.getValue();

            long commitNum = 0L;
            long insertSuccNum = 0L;
            long readNum = 0L;
            long executeNum = 0L;

            while (reader.hasNext()) {
                readNum++;
                Object value = reader.next();
                if (!(value instanceof Pair)) {
                    LOG.error("value not instanceof Pair");
                    continue;
                }

                Pair<Object, Object> pair = (Pair) value;
                Object exportKey = pair.key();
                Object exportValue = pair.value();

                Map<String, String> mapFields = getFields((Exportable) exportKey, (Exportable) exportValue, timestampName, timeValue, conf);
                if (null == mapFields) {
                    LOG.error("null == mapFeilds when exportKey=" + exportKey.toString() + ", exportValue=" + exportValue.toString());
                    return Constants.RET_INT_ERROR;
                }

                if (Constants.RET_INT_OK != addInsertSql(mapFields, field_type, mapAlaisNames == null ? null : mapAlaisNames.get(tableName))) {
                    continue;
                }

                commitNum++;
                executeNum++;
                if (executeNum >= DB_EXECUT_MODEL) {
                    executeNum = 0L;
                    int dbExc = exceModel();
                    //commit();
                    insertSuccNum += ((dbExc < 0) ? 0 : dbExc);
                }

                //每10000条数据执行一次commit
                if (commitNum >= DB_COMMIT_TIMES) {
                    commitNum = 0L;
                    commit();
                }

                if (readNum % 10000 == 0) {
                    LOG.info("read " + readNum + ", insert" + insertSuccNum + " from " + readFile);
                }
            }

            commitNum = 0L;
            int dbExc = exceModel();
            commit();
            insertSuccNum += ((dbExc < 0) ? 0 : dbExc);
            LOG.info("read " + readNum + ", insert" + insertSuccNum + " from " + readFile);

            totalcommitNum += commitNum;
            totalinsertSuccNum += insertSuccNum;
            totalreadNum += readNum;
        }

        closeModel();
        LOG.info("table " + tableName + ": total read " + totalreadNum + ", insertSucc " + totalinsertSuccNum + " records");
        return 0;
    }


}
