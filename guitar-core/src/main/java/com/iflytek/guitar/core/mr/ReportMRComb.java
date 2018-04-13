package com.iflytek.guitar.core.mr;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.data.ReportKey;
import com.iflytek.guitar.core.data.ReportValue;
import com.iflytek.guitar.core.util.ConstantConf;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.SafeDate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ReportMRComb {
    /**
     * 获取MR任务当前传入的文件路径
     */
    public static String getFilePath(Mapper.Context context) throws IOException {
        InputSplit split = context.getInputSplit();
        Class<? extends InputSplit> splitClass = split.getClass();
        FileSplit fileSplit = null;
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) split;
        } else if ("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit".equals(splitClass.getName())) {
            try {
                Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("getFilePath fail.");
            }
        }

        return fileSplit.getPath().toString();
    }

    public static class M_org extends Mapper<Object, Object, ReportKey, ReportValue> {

        List<ReportObject> ros = new ArrayList<ReportObject>();

        @Override
        protected void setup(Context context) throws IOException {
            List<String> alg_names = getAlgName(context.getConfiguration());
            String path = getFilePath(context);

            String strDate = context.getConfiguration().get(ConstantConf.CONF_TASK_TIME);
            Date dtStart = SafeDate.Format2Date(strDate, Constants.DATE_FORMAT_BASIC);

            String strFreq = context.getConfiguration().get(ConstantConf.CONF_TASK_FREQ);
            AlgDef.Frequence freq = AlgDef.Frequence.valueOf(strFreq);

            try {
                for (String alg_name : alg_names) {
                    ros.add(AlgDef.getAlgDefByName(alg_name, freq, context.getConfiguration())
                            .getReportObject(path, dtStart, freq, context.getConfiguration()));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new IOException(ex.getMessage());
            }
        }

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            for (ReportObject ro : ros) {
                ro.parse(key, value, context.getConfiguration());
                while (ro.next()) {
                    if (null == ro.getReportKey()) {
                        System.err.println("report key has null in " + ro.algdef.alg_name);
                    }
                    context.write(ro.getReportKey(), ro.getReportValue());
                }
            }
        }

        public static List<String> getAlgName(Configuration conf) throws IOException {
            String alg_str = conf.get(ConstantConf.ALG_LIST);
            if (null == alg_str) {
                throw new IOException("alg read from conf is empty!");
            }

            List<String> ret = new ArrayList<String>();
            String[] alg_arr = alg_str.split(",");
            for (String alg : alg_arr) {
                ret.add(alg);
            }

            return ret;
        }

    }

    public static class M_mid extends Mapper<Object, Object, ReportKey, ReportValue> {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            context.write((ReportKey) key, (ReportValue) value);
        }
    }


    public static class C extends Reducer<ReportKey, ReportValue, ReportKey, ReportValue> {
        @Override
        protected void reduce(ReportKey key, Iterable<ReportValue> values, Context context) throws IOException,
                InterruptedException {
            /* 合并 */
            ReportValue mergeValue = new ReportValue();
            for (ReportValue value : values) {
                mergeValue.merge(value);
            }

            /* 如果报表需要保存中间结果, 则写中间结果 */
            context.write(key, mergeValue);
        }
    }

    public static class R extends Reducer<ReportKey, ReportValue, ReportKey, ReportValue> {

        @Override
        protected void reduce(ReportKey key, Iterable<ReportValue> values, Context context) throws IOException,
                InterruptedException {
            /* 合并 */
            ReportValue mergeValue = new ReportValue();
            for (ReportValue value : values) {
                mergeValue.merge(value);
            }

            /* 如果报表需要保存中间结果, 则写中间结果 */
            context.write(key, mergeValue);
        }

    }
}
