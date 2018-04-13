package com.iflytek.guitar.share.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public final class UtilOper {
    private static final Log LOG = LogFactory.getLog(UtilOper.class);

    public static Boolean isTest() {
        String osName = System.getProperties().getProperty("os.name");
        if (osName.startsWith("win") || osName.startsWith("Win") ||
                osName.startsWith("mac") || osName.startsWith("Mac")) {
            return true;
        }

        return false;
    }

    public static String getDateDir(String freq, Date reportDate) {
        String dateDir = null;

        if (Constants.FREQ_HOURLY.equalsIgnoreCase(freq)) {
            dateDir = SafeDate.Date2Format(reportDate, Constants.DATE_FORMAT_HOURLY_DIR);
        } else {
            dateDir = SafeDate.Date2Format(reportDate, Constants.DATE_FORMAT_DAILY_DIR);
        }

        return dateDir;
    }

    public static String getReportDir(String freq, String reportName, String dateDir) {
        return freq + "/" + reportName + "/" + dateDir;
    }

    public static String getReportDir(String freq, String reportName, Date reportDate) {
        return getReportDir(freq, reportName, getDateDir(freq, reportDate));
    }

    public static String getOutputReportDir(String freq, String reportName, Date reportDate, String userName) {
        if (null == userName) {
            return "report/" + Constants.DIR_REPORT_SUBDIR_OUTPUT + "/" + getReportDir(freq, reportName, reportDate);
        } else {
            return "/user/" + userName + "/report/" + Constants.DIR_REPORT_SUBDIR_OUTPUT + "/" + getReportDir(freq, reportName, reportDate);
        }
    }

    public static String getOutputReportDir(Configuration conf, String freq, String reportName, Date reportDate) {
        if (conf.get(Constants.GUITAR_BASE_DIR) == null) {
            return getOutputReportDir(freq, reportName, reportDate, null);
        }

        return getBaseDirString(conf) + "/" + getOutputReportDir(freq, reportName, reportDate, null);
    }

    public static Path getBaseDir(Configuration conf) {
        String baseDir = conf.get(Constants.GUITAR_BASE_DIR);
        return new Path(baseDir);
    }

    public static String getBaseDirString(Configuration conf) {
        String baseDir = conf.get(Constants.GUITAR_BASE_DIR);
        return baseDir;
    }

    public static boolean install(Configuration conf, String srcDir, String destDir) throws IOException {
        if (null == conf) {
            LOG.error("null == conf");
            return false;
        }

        /* 判断源目录是否存在 */
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(srcDir);
        if (!fs.exists(srcPath)) {
            LOG.error("srcPath[" + srcPath + "] not exist.");
            return false;
        }

        /* 判断目的目录是否存在, 这里的目录是指报表输出数据总目录, 需要事先设置好 */
        Path destPath = new Path(destDir);
        if (!fs.exists(destPath)) {
            LOG.error("destPath[" + destPath + "] not exist.");
            return false;
        }

        /*
         * 迁移方法： 定义一个迁移目录队列, 用广度遍历的方法, 将各层子目录都加入迁移队列, 然后循环判断: -如果srcDir下当前待迁移的目录
         * 在 destDir下没有, 则整体迁移过去 -如果srcDir下当前待迁移的目录 在 destDir下存在, 则从队列中删除该目录,
         * 将其子目录在加入到迁移队列
         */
        List<String> lstMoveDir = Lists.newArrayList();
        lstMoveDir.add("");
        while (lstMoveDir.size() > 0) {
            String curDir = lstMoveDir.remove(0);
            Path subSrcPath = srcPath;
            Path subDestPath = destPath;

            /* 当前已子目录时, 先确定好源子目录、目的子目录的路径 */
            if (curDir.length() > 0) {
                subSrcPath = new Path(srcPath, curDir);
                subDestPath = new Path(destPath, curDir);
            }

            /*
             * 只要srcPath下的子目录destPath下没有时, 整体迁移过来 首次执行时, subDestPath=destPath,
             * 肯定是存在的;
             */
            if (!fs.exists(subDestPath)) {
                fs.rename(subSrcPath, subDestPath);
                continue;
            }

            /*
             * 目录都存在的话, 如果是报表的current子目录, 则将current整体删除、迁移 防止任务重跑时,
             * reduce数量变化引起老数据删除不全
             */
            if (subDestPath.getName().equals(Constants.DIR_REPORT_DIR_CURRENT)) {
                /*
                 * 异常判断: 在报表输出的目录层中, 理论上算法名这一层目录也有可能叫"current" 目录层如:
                 * ${destPath}/interm/hourly/current/2014-06-18/09/current
                 */
                if (subDestPath.depth() - destPath.depth() > 3) {
                    fs.delete(subDestPath, true);
                    fs.rename(subSrcPath, destPath);
                    continue;
                }
            }

            /*
             * 遍历子文件(目录), 按下面方式处理: 1 如果是子目录, 则把子目录加入迁移队列 2 如果是子文件, 则把子文件直接迁移过来;
             * 报表输出目录不会出现这个情况.
             */
            FileStatus[] aStatus = fs.listStatus(subSrcPath);
            for (FileStatus tmpStatus : aStatus) {
                Path tmpPath = tmpStatus.getPath();
                if (tmpStatus.isDirectory()) {
                    if (curDir.length() > 0) {
                        //这里本来是按照“天”文件夹来删除重名目录的，但是对小时不适用。小时数据需要按照“小时”文件夹重名来删除
                        String tmpDir = curDir + "/" + tmpPath.getName();
                        lstMoveDir.add(tmpDir);
                        Path tmppath = new Path(tmpDir);
                        if (tmppath.depth() == 4 && !"hourly".equalsIgnoreCase(tmpDir.split("/")[1])) {
                            Path deletepath = new Path(destPath, tmppath);
                            if (fs.exists(deletepath)) {
                                fs.delete(deletepath, true);
                            }
                        } else if (tmppath.depth() == 5 && "hourly".equalsIgnoreCase(tmpDir.split("/")[1])) {
                            Path deletepath = new Path(destPath, tmppath);
                            if (fs.exists(deletepath)) {
                                fs.delete(deletepath, true);
                            }
                        }
                    } else {
                        lstMoveDir.add(tmpPath.getName());
                    }
                } else {
                    /* 如果是"_SUCCESS"文件, 则忽略 */
                    String fileName = tmpPath.getName();
                    if (Constants.FILENAME_SUCCESS.equals(fileName)) {
                        continue;
                    }

                    /* 如果目的目录下有此文件, 则先删除, 在迁移 */
                    Path tmpDestPath = new Path(subDestPath, fileName);
//                    if (fs.exists(tmpDestPath))
//                    {
//                        fs.delete(tmpDestPath, false);
//                    }

                    fs.rename(tmpPath, new Path(subDestPath, fileName));
                }
            }
        }

        return true;
    }

}
