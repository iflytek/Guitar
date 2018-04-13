package com.iflytek.guitar.core.mr;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.data.Field;
import com.iflytek.guitar.core.data.ReportKey;
import com.iflytek.guitar.core.data.ReportValue;
import com.iflytek.guitar.core.util.ConstantConf;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.SafeDate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ReportMRCube {
    private static final Log LOG = LogFactory.getLog(ReportMRCube.class);

    public static class M extends Mapper<ReportKey, ReportValue, ReportKey, ReportValue> {

        Map<String, ReportObject> ros = new HashMap<String, ReportObject>();
        String alg_name = "";
        Map<ReportKey, ReportValue> cuboidN = new HashMap<ReportKey, ReportValue>();
        int treeHigh = 0;
        //List<String> algKeys = null;
        int records_of_alg = 0;
        Field[] fields = null;
        ReportObject curReportObject = null;

        @Override
        protected void setup(Context context) throws IOException {
            List<String> alg_names = ReportMRComb.M_org.getAlgName(context.getConfiguration());

            String path = ReportMRComb.getFilePath(context);
            String strDate = context.getConfiguration().get(ConstantConf.CONF_TASK_TIME);
            Date dtStart = SafeDate.Format2Date(strDate, Constants.DATE_FORMAT_BASIC);

            String strFreq = context.getConfiguration().get(ConstantConf.CONF_TASK_FREQ);
            AlgDef.Frequence freq = AlgDef.Frequence.valueOf(strFreq);

            try {
                for (String alg_name : alg_names) {
                    LOG.info("Init alg of " + alg_name + " in set_up");
                    ros.put(alg_name, AlgDef.getAlgDefByName(alg_name, freq, context.getConfiguration())
                            .getReportObject(path, dtStart, freq, context.getConfiguration()));

                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new IOException(ex.getMessage());
            }
        }

        @Override
        protected void map(ReportKey key, ReportValue value, Context context) throws IOException, InterruptedException {
            // 通过设置ReportKey的排序特性，保证同一个报表的数据是连续的
            // More information at : http://www.infoq.com/cn/articles/apache-kylin-algorithm/
            if ("".equals(alg_name)) {
                LOG.info("Read fist record and alg_name is empty");
                LOG.info("-------------Now to read data of Alg " + key.reportName);
                curReportObject = ros.get(key.reportName);
                //treeHigh = ros.get(key.reportName).sortedDims.size();
                treeHigh = curReportObject.getCubeDimHigh();
                fields = curReportObject.getFields();
                alg_name = key.reportName;
                //algKeys = ros.get(key.reportName).sortedDims;
            } else if (!key.reportName.equals(alg_name) || records_of_alg == 10000) {
                LOG.info("-------------Read data over of Alg " + alg_name);
                List<Integer> dims = curReportObject.getCubeDims();
//                List<Integer> dims = new ArrayList<Integer>(treeHigh);
//                for (int i=0; i<treeHigh; i++){
//                    dims.add(treeHigh-1-i);
//                }
                LOG.info("records_of_alg is " + records_of_alg);
                records_of_alg = 0;
                // 构造cube
                traverseCuboidTree(cuboidN, treeHigh, dims, context);

                // 重新初始化
                if (!key.reportName.equals(alg_name)) {
                    curReportObject = ros.get(key.reportName);
                    fields = curReportObject.getFields();
                    alg_name = key.reportName;
                    treeHigh = curReportObject.getCubeDimHigh();
                }
                cuboidN = new HashMap<ReportKey, ReportValue>();
                //algKeys = ros.get(key.reportName).sortedDims;

                LOG.info("-------------Now to read data of Alg " + key.reportName);
            }

            records_of_alg++;
            ReportValue rv = cuboidN.get(key);
            if (null == rv) {
                rv = new ReportValue();
                cuboidN.put(key, rv);
            }
            rv.merge(value);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (null != curReportObject) {
                List<Integer> dims = curReportObject.getCubeDims();
//            List<Integer> dims = new ArrayList<Integer>(treeHigh);
//            for (int i=0; i<treeHigh; i++){
//                dims.add(treeHigh - 1 - i);
//            }
                traverseCuboidTree(cuboidN, treeHigh, dims, context);
            }
        }

        private void traverseCuboidTree(Map<ReportKey, ReportValue> root, int high, List<Integer> algKeys, Context context)
                throws IOException, InterruptedException {

            LOG.info("~~~~~~~~~~~~~~~~~Now in traverseCuboidTree, high is " + high + " and " + algKeys);

            if (0 == high) {
                //LOG.info(">>>>>>>>>>>>>>>Now to write data to fileSystem");
                for (Map.Entry<ReportKey, ReportValue> entry : root.entrySet()) {
                    context.write(entry.getKey(), entry.getValue());
                }
                //LOG.info(">>>>>>>>>>>>>>>Write data to fileSystem over of records " + root.size());
            } else {
                for (int i = 0; i < high; i++) {
                    Map<ReportKey, ReportValue> node = new HashMap<ReportKey, ReportValue>();
                    int key_del = algKeys.get(i);
                    List<Integer> algKeys_subtree = clone(algKeys);
                    algKeys_subtree.remove(i);

                    //LOG.info("^^^^^^^^^^^^^^^Now to generate child node " + i + " of " + algKeys);
                    for (Map.Entry<ReportKey, ReportValue> entry : root.entrySet()) {
                        ReportKey rk = entry.getKey().clone();
                        //rk.set(key_del, ConstantsAlg.CHAR_ALL);
                        if (fields[key_del].setALL(rk)) {
                            ReportValue rv = node.get(rk);
                            if (null == rv) {
                                rv = new ReportValue();
                                node.put(rk, rv);
                            }
                            rv.merge(entry.getValue());
                        }
                    }
                    //LOG.info("^^^^^^^^^^^^^^^Over to generate child node " + i + " of " + algKeys);

                    traverseCuboidTree(node, i, algKeys_subtree, context);
                }

                //LOG.info(">>>>>>>>>>>>>>>Now to write data to fileSystem");
                for (Map.Entry<ReportKey, ReportValue> entry : root.entrySet()) {
                    context.write(entry.getKey(), entry.getValue());
                }
                //LOG.info(">>>>>>>>>>>>>>>Write data to fileSystem over of records " + root.size());
            }
        }

        public List<Integer> clone(List<Integer> o) {
            if (null == o) {
                return null;
            }
            List<Integer> ret = new ArrayList<Integer>();
            for (int i = 0; i < o.size(); i++) {
                ret.add(o.get(i));
            }
            return ret;
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
