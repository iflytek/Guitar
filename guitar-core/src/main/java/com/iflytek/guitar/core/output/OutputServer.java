package com.iflytek.guitar.core.output;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.alg.Output;
import com.iflytek.guitar.core.alg.RunAlgs;
import com.iflytek.guitar.core.data.ReportKey;
import com.iflytek.guitar.core.export.Export2Phoenix;
import com.iflytek.guitar.core.main.ExportMain;
import com.iflytek.guitar.export.Export2DB;
import com.iflytek.guitar.oozie.OozieMain;
import com.iflytek.guitar.share.db.DBConnect;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.UtilOper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class OutputServer extends OozieMain implements Runnable {
    private static final Log LOG = LogFactory.getLog(OutputServer.class);

    private static OutputServer instance = null;

    private boolean isOutputing = false;
    private volatile boolean quit = false;

    private Queue<RunAlgs> outputQueue = new ArrayBlockingQueue<RunAlgs>(1000);
    public List<String> erralg = new ArrayList<String>();

    private Configuration conf;

    private OutputServer() {
    }

    // 单例
    public static OutputServer get(Configuration conf) {
        if (null == instance) {
            instance = new OutputServer();
        }
        if (null == instance.conf) {
            instance.conf = conf;
        }

        return instance;
    }

    // override of Runnable
    @Override
    public void run() {
        while (!quit) {
            isOutputing = true;
            RunAlgs runalg = outputQueue.poll();
            if (null != runalg) {

                for (AlgDef algdef : runalg.algs.values()) {
                    String reportName = algdef.alg_name;

                    ReportKey.algFeildsDef.put(algdef.alg_name, algdef.getSortedDimensionNameList());

                    String ip = ((Output.OParamImpl) algdef.output.oparam).ip;
                    Long port = new Long(((Output.OParamImpl) algdef.output.oparam).port);
                    String dbName = ((Output.OParamImpl) algdef.output.oparam).dbName;
                    String userName = ((Output.OParamImpl) algdef.output.oparam).user;
                    String passwd = ((Output.OParamImpl) algdef.output.oparam).passwd;
                    String tableName;
                    if (null == algdef.customTableName || 0 == algdef.customTableName.size()
                            || null == algdef.customTableName.get(runalg.freq)) {
                        tableName = algdef.getSimpleAlgName() + runalg.freq;
                    } else {
                        tableName = algdef.customTableName.get(runalg.freq);
                    }
                    String inputDir = UtilOper.getOutputReportDir(conf, runalg.freq.toString(), reportName, runalg.startDate);
                    Date timeValue = runalg.startDate;
                    String timestampName = null;
                    // 支持不配置数据库报表别名信息时，直接按照报表的结果入库，否则修改数据库别名
                    Map<String, Map<String, String>> mapAlaisNames = null;
                    try {
                        mapAlaisNames = ExportMain.parseAliasConfig(algdef.getAlgAliasPath(conf), conf);
                    } catch (IOException e) {
                        e.printStackTrace();
                        LOG.warn("parseAliasConfig error!");
                    }

                    /*================================================入库============================================*/
                    int iRet = Constants.RET_INT_ERROR;
                    if (Output.OType.mysql == algdef.output.otype) { // 入库到mysql
                        String driver = "mysql";

                        // 建立数据库连接
                        DBConnect dbConn = new DBConnect(driver, ip, port, dbName, userName, passwd);
                        if (null == dbConn.getConnection()) {
                            LOG.error("exportDB: null == dbConn.getConnection()");
                            erralg.add(reportName + "-" + runalg.freq + "-" + runalg.startDate);
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

                        // 入库
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
                    }

                    if (iRet != Constants.RET_INT_OK) {
                        LOG.error("exportDB: " + tableName + " export fail!");
                        if (iRet != Constants.RET_INT_ERROR_NORD) {
                            erralg.add(reportName + "-" + runalg.freq + "-" + runalg.startDate);
                        }
                    } else {
                        LOG.info("export2DB.export " + inputDir + " to " + tableName + " OK!");
                    }

                }
            }

            isOutputing = false;

            try {
                Thread.sleep(2 * 1000L);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

    }

    protected static void addAlgToOutputServer(AlgDef algDef,
                                               AlgDef.Frequence freq,
                                               boolean isLoop,
                                               Date start,
                                               boolean force,
                                               Configuration conf)
            throws IOException, InterruptedException {
        if (isLoop && freq.bigger(algDef.minFreq)) {
            AlgDef.Frequence sonFreq = freq.getChildFreq();

            Date beg = new Date();
            beg.setTime(start.getTime());

            Date end = freq.getEndDate(start);

            while (beg.before(end)) {
                addAlgToOutputServer(algDef, sonFreq, isLoop, beg, force, conf);
                beg = sonFreq.getEndDate(beg);
            }
        }

        RunAlgs runAlgs = new RunAlgs(algDef, start, freq, force, conf);
        OutputServer.get(conf).put(runAlgs);
    }

    public void put(RunAlgs runalgs) throws InterruptedException {
        outputQueue.add(runalgs);
    }

    public boolean isFinished() {
        return (outputQueue.isEmpty() && !isOutputing);
    }

    public void quit() {
        quit = true;
    }

}
