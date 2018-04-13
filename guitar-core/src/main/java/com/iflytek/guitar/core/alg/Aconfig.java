package com.iflytek.guitar.core.alg;

import com.iflytek.guitar.share.db.DBConnect;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Aconfig {
    public String type;
    public String id;

    public String sql;
    public Output.OParamImpl param;

    public String path;

    public void setParam(Map<String, String> params) throws IOException {
        param = new Output.OParamImpl();

        param.ip = params.get("ip");
        param.port = Integer.parseInt(params.get("port"));
//        param.cluster = params.get("cluster");

        param.user = params.get("user");
        param.passwd = params.get("passwd");

        param.dbName = params.get("dbName");
        //((OParamMysql)oparam).tableName = params.get("tableName");
    }

    public void read2File(Path conf_intern, Configuration conf) throws IOException, SQLException {
        FileSystem fs = conf_intern.getFileSystem(conf);
        if ("DB".equalsIgnoreCase(type)) {
            if (!fs.exists(conf_intern)) {
                String driver = "mysql";

                /* 建立数据库连接 */
                DBConnect dbConn = new DBConnect(driver, param.ip, (long) param.port, param.dbName,
                        param.user, param.passwd);
                if (null == dbConn.getConnection()) {
                    throw new IOException("exportDB: null == dbConn.getConnection()");
                }
                Connection conn = dbConn.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);

                FSDataOutputStream hdfsOutStream = fs.create(conf_intern);
                boolean isFirst = true;
                while (rs.next()) {
                    if (!isFirst) {
                        hdfsOutStream.write('\n');
                    }
                    isFirst = false;
                    int columncount = rs.getMetaData().getColumnCount();
                    for (int i = 1; i < columncount; i++) {
                        hdfsOutStream.write(rs.getString(i).getBytes());
                        hdfsOutStream.write('\t');
                    }
                    hdfsOutStream.write(rs.getString(columncount).getBytes());
                }
                hdfsOutStream.close();
            } else {
                System.out.println("config : " + conf_intern + " is already existed, no updated it!");
            }
        } else if ("HDFS".equalsIgnoreCase(type)) {
            if (!fs.exists(new Path(path))) {
                throw new IOException("File " + path + " do not exists!");
            }

            FSDataOutputStream hdfsOutStream = fs.create(conf_intern);
            boolean isFirst = true;

            FSDataInputStream inputStream = fs.open(new Path(path));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String lineTxt;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (!isFirst) {
                    hdfsOutStream.write('\n');
                }
                isFirst = false;
                hdfsOutStream.write(lineTxt.getBytes());
            }
            inputStream.close();
            hdfsOutStream.close();

        } else {
            throw new IOException("Unsupported config type " + type);
        }
    }

    public List<String> readFromFile(Path conf_intern, Configuration conf) throws IOException {
        FileSystem fs = conf_intern.getFileSystem(conf);
        if (!fs.exists(conf_intern)) {
            throw new IOException("File " + conf_intern.toString() + " do not exists!");
        }

        FSDataInputStream inputStream = fs.open(conf_intern);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String lineTxt;
        List<String> ret = new ArrayList<String>();
        while ((lineTxt = bufferedReader.readLine()) != null) {
            ret.add(lineTxt);
        }
        inputStream.close();

        return ret;
    }

    @Override
    public String toString() {
        return "Aconfig{" +
                "type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", sql='" + sql + '\'' +
                ", param=" + param +
                ", path='" + path + '\'' +
                '}';
    }

}
