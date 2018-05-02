package com.iflytek.guitar.core.main;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.alg.Output;
import com.iflytek.guitar.core.data.ReportKey;
import com.iflytek.guitar.core.export.Export2Phoenix;
import com.iflytek.guitar.export.Export2DB;
import com.iflytek.guitar.share.db.DBConnect;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.SafeDate;
import com.iflytek.guitar.share.utils.UtilOper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class ExportMain {
    private static final Log LOG = LogFactory.getLog(ExportMain.class);

    public static Map<String, Map<String, String>> parseAliasConfig(Path filePath, Configuration conf) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(filePath) || !fs.isFile(filePath)) {
            LOG.error("config error: " + filePath + "do not exsit");
            return null;
        }

        Map<String, Map<String, String>> mapAliasName = Maps.newHashMap();

        FSDataInputStream fInput = fs.open(filePath);
        String strLine = null;
        while (null != (strLine = fInput.readLine())) {
            String[] tmpInfo = strLine.split("=");
            if (null == tmpInfo || tmpInfo.length != 2) {
                continue;
            }

    		/* tmpInfo  如 UserSessDaily.USER=dvc_info
             * key      如 UserSessDaily.USER
    		 * value    如 dvc_info
    		 **/
            String key = tmpInfo[0].trim();
            String alaisName = tmpInfo[1].trim();

            tmpInfo = key.split("\\.");
            if (null == tmpInfo || tmpInfo.length != 2) {
                continue;
            }

            String tableName = tmpInfo[0].trim();
            /*与数据库列名转小写对应*/
            String fieldName = tmpInfo[1].trim();

    		/* 此时, tableName表中的fieldName字段在输入记录中的字段名为alaisName */
            Map<String, String> tmpMap = mapAliasName.get(tableName);
            if (null == tmpMap) {
                tmpMap = new HashMap<String, String>();
                mapAliasName.put(tableName, tmpMap);
            }
            tmpMap.put(fieldName, alaisName);
        }

        fInput.close();

        return mapAliasName;
    }

    public static void main(String[] args) throws Exception {
        if (null == args || args.length < 3) {
            throw new Exception("args error: <algs> <start_time> <freq> <-is_strictDataInsert b>\n"
                    + "algs: describe what reports need to run .\n"
                    + "start_time: describe the timestamp info for all reports.\n"
                    + "freq: describe the running period of reports.\n"
                    + "-is_strictDataInsert b: describe is_strict data insert inspection, default false.\n"
            );
        }

        List<String> erralg = new ArrayList<String>();

        Configuration conf = new Configuration();

        String runtime_user = UserGroupInformation.getCurrentUser().getUserName();
        LOG.info("Current user's full name is : " + runtime_user);

        boolean is_strictDataInsert = false;
        if (args.length > 3) {
            for (int idx = 3; idx < args.length; idx++) {
                if ("-is_strictDataInsert".equalsIgnoreCase(args[idx])) {
                    if (++idx == args.length) {
                        throw new IllegalArgumentException("is_strict data insert inspection not specified in -is_strictDataInsert");
                    }
                    is_strictDataInsert = Boolean.valueOf(args[idx]);
                }
            }
        }

        // 是否严格数据插入检查，若是，则只要有一条数据有问题或数据库服务问题则立即抛出异常
        conf.setBoolean("is_strictDataInsert", is_strictDataInsert);

        String algs_s = args[0];
        String[] algs_list = algs_s.split(",");
        if (null == algs_list || algs_list.length == 0) {
            throw new Exception("alg must not be empty!");
        }

        Date startDate = SafeDate.Format2Date(args[1].trim(), Constants.DATE_FORMAT_BASIC);

        AlgDef.Frequence freq = AlgDef.Frequence.valueOf(args[2]);

        Map<String, AlgDef> algs = new HashMap<String, AlgDef>();
        for (String alg : algs_list) {
            algs.put(alg, AlgDef.getAlgDefByName(alg, freq, conf));
        }

        for (AlgDef algdef : algs.values()) {

            String reportName = algdef.alg_name;

            ReportKey.algFeildsDef.put(algdef.alg_name, algdef.getSortedDimensionNameList());

            String ip = ((Output.OParamImpl) algdef.output.oparam).ip;
            Long port = new Long(((Output.OParamImpl) algdef.output.oparam).port);
            String dbName = ((Output.OParamImpl) algdef.output.oparam).dbName;
            String userName = ((Output.OParamImpl) algdef.output.oparam).user;
            String passwd = ((Output.OParamImpl) algdef.output.oparam).passwd;
            String tableName;
            if (null == algdef.customTableName || 0 == algdef.customTableName.size()
                    || null == algdef.customTableName.get(freq)) {
                tableName = algdef.getSimpleAlgName() + freq;
            } else {
                tableName = algdef.customTableName.get(freq);
            }
            String inputDir = UtilOper.getOutputReportDir(conf, freq.toString(), reportName, startDate);
            Date timeValue = startDate;
            String timestampName = null;
            // 支持不配置数据库报表别名信息时，直接按照报表的结果入库，否则修改数据库别名
            Map<String, Map<String, String>> mapAlaisNames = null;
            try {
                mapAlaisNames = ExportMain.parseAliasConfig(algdef.getAlgAliasPath(conf), conf);
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("parseAliasConfig error!");
            }

            int iRet = Constants.RET_INT_ERROR;
            if (Output.OType.mysql == algdef.output.otype) { // 入库到mysql
                String driver = "mysql";

                // 建立数据库连接
                DBConnect dbConn = new DBConnect(driver, ip, port, dbName, userName, passwd);
                if (null == dbConn.getConnection()) {
                    LOG.error("exportDB: null == dbConn.getConnection()");
                    erralg.add(reportName + "-" + freq + "-" + startDate);
                    continue;
                }

                // 入库
                Export2DB export2DB = new Export2DB();
                try {
                    iRet = export2DB.export(dbConn, tableName, inputDir, timeValue, timestampName, mapAlaisNames, conf);
                } catch (Exception e) {
                    iRet = Constants.RET_INT_ERROR;
                    e.printStackTrace();
                }

                // 关闭链接
                dbConn.closeConn();

            } else if (Output.OType.phoenix == algdef.output.otype) { // 入库到phoenix
                String driver = "phoenix";

                Export2Phoenix export2Phoenix = new Export2Phoenix(ip);
                try {
                    iRet = export2Phoenix.export(tableName, inputDir, timeValue, timestampName, mapAlaisNames, conf);
                } catch (IOException e) {
                    iRet = Constants.RET_INT_ERROR;
                    e.printStackTrace();
                } catch (SQLException e) {
                    iRet = Constants.RET_INT_ERROR;
                    e.printStackTrace();
                }

            } else { // 入库到其他类型，暂不支持
                LOG.error("Unsupported OType of " + algdef.output.otype);
                erralg.add(reportName + "-" + freq + "-" + startDate);
            }

            if (iRet != Constants.RET_INT_OK) {
                LOG.error("exportDB: " + tableName + " export fail!");
                if (iRet != Constants.RET_INT_ERROR_NORD) {
                    erralg.add(reportName + "-" + freq + "-" + startDate);
                }
            } else {
                LOG.info("export2DB.export " + inputDir + " to " + tableName + " OK!");
            }

        }

    }
}
