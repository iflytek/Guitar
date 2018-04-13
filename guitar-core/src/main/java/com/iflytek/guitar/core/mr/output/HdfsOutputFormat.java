package com.iflytek.guitar.core.mr.output;

import com.iflytek.guitar.core.data.ReportKey;
import com.iflytek.guitar.core.util.ConstantConf;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.SafeDate;
import com.iflytek.guitar.share.utils.UtilOper;
import com.iflytek.guitar.share.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.guitar.share.avro.mapreduce.output.MultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Date;

public class HdfsOutputFormat extends FileOutputFormat<ReportKey, Object> {

    public class ReportWriter extends RecordWriter<ReportKey, Object> {
        private final MultipleOutputs mos;
        private final Configuration conf;
        private String dateDir = null;
        private String freq = null;

        public ReportWriter(TaskAttemptContext job) throws IOException {
            mos = new MultipleOutputs(job);
            conf = job.getConfiguration();

            freq = conf.get(ConstantConf.CONF_TASK_FREQ);

            String strDate = conf.get(ConstantConf.CONF_TASK_TIME);

            Date reportDate = SafeDate.Format2Date(strDate, Constants.DATE_FORMAT_BASIC);
            if (null == reportDate) {
                throw new IOException("strDate[" + strDate + "] invalid.");
            }

            dateDir = UtilOper.getDateDir(freq, reportDate);
            if (null == dateDir) {
                throw new IOException("null == dateDir");
            }
        }

        @Override
        public void write(ReportKey key, Object value) throws IOException, InterruptedException {
            /* 判断是存中间数据目录、还是输出数据目录 */
            boolean isInterm = false;

            String subDir = isInterm ? Constants.DIR_REPORT_SUBDIR_INTERM : Constants.DIR_REPORT_SUBDIR_OUTPUT;
            String reportName = key.getReportName();
            if (null == reportName) {
                throw new IOException("null == reportName");
            }

            subDir += "/" + UtilOper.getReportDir(freq, reportName, dateDir);
            mos.write(AvroPairOutputFormat.class, subDir, key, value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            mos.close();
        }

    }

    @Override
    public RecordWriter<ReportKey, Object> getRecordWriter(TaskAttemptContext job) throws IOException {
        return new ReportWriter(job);
    }

}
