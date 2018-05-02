package com.iflytek.guitar.core.main;

import com.iflytek.guitar.core.alg.RunAlgs;
import com.iflytek.guitar.core.output.OutputServer;
import com.iflytek.guitar.core.schedule.Scheduler;
import com.iflytek.guitar.core.util.ConstantConf;
import com.iflytek.guitar.oozie.OozieMain;
import com.iflytek.guitar.oozie.OozieToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Arrays;

public class ReportMain extends OozieMain {

    public ReportMain() {
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean is_todb = true;
        boolean is_strictDataInsert = false;
        if (args.length > 4) {
            for (int idx = 4; idx < args.length; idx++) {
                if ("-is_todb".equalsIgnoreCase(args[idx])) {
                    if (++idx == args.length) {
                        throw new IllegalArgumentException("is to db not specified in -is_todb");
                    }
                    is_todb = Boolean.valueOf(args[idx]);
                } else if ("-is_strictDataInsert".equalsIgnoreCase(args[idx])) {
                    if (++idx == args.length) {
                        throw new IllegalArgumentException("is_strict data insert inspection not specified in -is_strictDataInsert");
                    }
                    is_strictDataInsert = Boolean.valueOf(args[idx]);
                }
            }
        }

        String runtime_user = UserGroupInformation.getCurrentUser().getUserName();
        LOG.info("Current user's full name is : " + runtime_user);

//        getConf().set("mapreduce.framework.name", "local");

        String storeType = this.getConf().get(ConstantConf.OUTPUT_STORE_TYPE, "hdfs");

        // 是否严格数据插入检查，若是，则只要有一条数据有问题或数据库服务问题则立即抛出异常
        this.getConf().setBoolean("is_strictDataInsert", is_strictDataInsert);

        // 加载用户配置的报表算法
        RunAlgs runAlgs = new RunAlgs(args, this.getConf());

        // 初始化报表算法调度器
        Scheduler scheduler = new Scheduler(runAlgs, this.getConf());

        if (is_todb) {
            if ("hdfs".equals(storeType)) {
                // 启动入库服务
                new Thread(OutputServer.get(this.getConf())).start();
            }
        }

        try {
            // 启动调度器，开始执行报表算法，此处阻塞执行，直到所有报表完成
            scheduler.run();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            if (is_todb) {
                if ("hdfs".equals(storeType)) {
                    // 等待所有报表入库完成
                    while (!OutputServer.get(this.getConf()).isFinished()) {
                        LOG.info("Now waiting output to finished!");
                        Thread.sleep(20 * 1000L);
                    }

                    // 所有报表入库完成，退出入库服务
                    OutputServer.get(this.getConf()).quit();

                    // 显示入库失败的信息
                    if (OutputServer.get(this.getConf()).erralg.size() != 0) {
                        throw new Exception("ExportToDb failed:" + OutputServer.get(this.getConf()).erralg.toString());
                    }
                }
            } else {
                LOG.info("not to db");
            }
        }

        return 0;

    }

    public static void main(String[] args) throws Exception {
        if (null != args) {
            LOG.info(Arrays.toString(args));
        }

        int res = OozieToolRunner.run(new Configuration(), new ReportMain(), args);
        System.exit(res);
    }
}
