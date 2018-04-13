package com.iflytek.guitar.core.mr;

import com.iflytek.guitar.core.alg.Aconfig;
import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.data.Field;
import com.iflytek.guitar.core.data.FieldIgnore;
import com.iflytek.guitar.core.data.ReportKey;
import com.iflytek.guitar.core.data.ReportValue;
import com.iflytek.guitar.core.data.analysisformat.*;
import com.iflytek.guitar.core.data.dataformat.DataRecord;
import com.iflytek.guitar.core.hpath.PathDef;
import com.iflytek.guitar.core.util.ScriptMap;
import com.iflytek.guitar.core.util.ScriptOper;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.UtilOper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.*;

public abstract class ReportObject {
    public AlgDef algdef;
    protected ReportKey rk;
    protected ReportValue rv;
    protected Map<String, Class<? extends AnalysisFormat>> tagAnaFormatMap = new HashMap<String, Class<? extends AnalysisFormat>>();
    public List<String> sortedDims = null;
    public Field[] fields = null;

    public ReportObject(AlgDef algd) throws IOException {
        algdef = algd;
        initTagAnaFormatMap();
    }

    public void initTagAnaFormatMap() throws IOException {
        for (AlgDef.Target targ : algdef.targets) {
            switch (targ.type) {
                case SUM:
                    tagAnaFormatMap.put(targ.name, SumFormat.class);
                    break;
                case MAX:
                    tagAnaFormatMap.put(targ.name, MaxFormat.class);
                    break;
                case MIN:
                    tagAnaFormatMap.put(targ.name, MinFormat.class);
                    break;
                case AVG:
                    tagAnaFormatMap.put(targ.name, AvgFormat.class);
                    break;
                case VALUEDIST:
                    tagAnaFormatMap.put(targ.name + "#" + targ.param, ValueDistFormat.class);
                    break;
                case RANGEDIST:
                    tagAnaFormatMap.put(targ.name + "#" + targ.param, RangeDistFormat.class);
                    break;
                case UVBITMAP:
                    tagAnaFormatMap.put(targ.name, UVBitmapFormat.class);
                    break;
                case UV_HYPERLOGLOG:
                    tagAnaFormatMap.put(targ.name, HyperLogLogFormat.class);
                    break;
                case UV_HYPERLOGLOGPLUS:
                    tagAnaFormatMap.put(targ.name, HyperLogLogPlusFormat.class);
                    break;

                case PV:
                case UV:
                case UV_ACC:
                case PERCENTDIST:
                default:
                    throw new IOException("Unsupported target type: " + targ.type);
            }
        }
    }

    /*
     * 设置输入数据并计算出对应的ReportKey和ReportValue
     */
    public abstract void parse(Object key, Object value, Configuration conf) throws IOException;

    public abstract boolean next() throws IOException;

    public abstract int getCubeDimHigh();

    public abstract List<Integer> getCubeDims();

    public ReportKey getReportKey() {
        return rk;
    }

    public ReportValue getReportValue() {
        return rv;
    }

    public abstract Field[] getFields();


    public static class ReportObjectGroovy extends ReportObject {
        DataRecord dr;
        public List<ScriptMap> lstPreProcRlt = Lists.newArrayList();
        int i = -1;
        //        MetaMethod preProcMethod = null;

        public ReportObjectGroovy(AlgDef algd, String inputFile, Date taskTime, AlgDef.Frequence freq, Configuration conf) throws IOException {
            super(algd);

//            List<MetaMethod> lstMethod = algd.parseGroovyObject.getMetaClass()
//                    .respondsTo(algd.parseGroovyObject, "parse");
//            if (null == lstMethod || lstMethod.size() <= 0)
//            {
//                throw  new IOException("load parse fail from " + algd.getAlgParsePath(conf));
//            } else
//            {
//                preProcMethod = lstMethod.get(0);
//            }

            dr = DataRecord.getDataRecord(algdef.input.file_type, algdef.input.data_type);

            sortedDims = algd.getSortedDimensionNameList();

            Map<String, Object> conf_out = new HashMap<String, Object>();
            Map<String, List<String>> conf_in = new HashMap<String, List<String>>();
            if (null != inputFile && null != taskTime && null != freq) {
                // read config info from hdfs, and call map_setup method.
                if (null != algd.aconfigs && algd.aconfigs.size() > 0) {
                    for (Aconfig aconfig : algd.aconfigs) {
                        Path conf_intern = PathDef.get(PathDef.CONFIG_INTER).parseDate(taskTime).
                                parseFreq(freq).parseID(aconfig.id).toPath();
                        if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
                            conf_intern = new Path(UtilOper.getBaseDir(conf), conf_intern);
                        }
                        List<String> conf_in_item = aconfig.readFromFile(conf_intern, conf);
                        if (null != conf_in_item && conf_in_item.size() > 0) {
                            conf_in.put(aconfig.id, conf_in_item);
                        }
                    }

                    for (Map.Entry<String, List<String>> entry : conf_in.entrySet()) {
                        conf_out.put(entry.getKey(), entry.getValue());
                    }

                }
            }
            fields = algdef.cuboid.getFields(sortedDims, conf_out);
            initScriptOper(algdef.parseGroovyObject, inputFile, taskTime, conf_out);
        }

        @Override
        public void parse(Object key, Object value, Configuration conf) throws IOException {
            try {
                lstPreProcRlt = Lists.newArrayList();
                i = -1;
                DataRecord operData = dr.parseData(key, value);
//                preProcMethod.invoke(algdef.parseGroovyObject, new Object[] { operData, lstPreProcRlt });
                algdef.parseGroovyObject.parse(operData, lstPreProcRlt);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("throw exception in " + algdef.getAlgParsePath(conf));
            }
        }

        @Override
        public boolean next() throws IOException {
            i++;
            if (i >= lstPreProcRlt.size()) {
                return false;
            }
            ScriptMap smap = lstPreProcRlt.get(i);
            rk = new ReportKey(algdef.alg_name, sortedDims.size());
            rv = new ReportValue();

            for (int i = 0; i < sortedDims.size(); i++) {
                rk.set(i, (String) smap.get(sortedDims.get(i)));
            }

            for (Map.Entry<String, Class<? extends AnalysisFormat>> en : tagAnaFormatMap.entrySet()) {
                String key = (String) en.getKey();
                Class<? extends AnalysisFormat> af = en.getValue();
                AnalysisFormat analysisf = null;
                try {
                    String param = null;
                    if (key.split("#").length == 2) {
                        String[] mess = key.split("#");
                        key = mess[0];
                        param = mess[1];
                    }
                    Object v = smap.get(key);
                    if (null != v) {
                        if ("RangeDistFormat".equals(af.getSimpleName())) {
                            analysisf = new RangeDistFormat(param);
                            analysisf.setValue(v);
                        } else if ("ValueDistFormat".equals(af.getSimpleName())) {
                            analysisf = new ValueDistFormat(param);
                            analysisf.setValue(v);
                        } else if ("HyperLogLogPlusFormat".equals(af.getSimpleName())) {
                            analysisf = new HyperLogLogPlusFormat(algdef.mainFreq);
                            analysisf.setValue(v);
                        } else {
                            analysisf = af.newInstance();
                            analysisf.setValue(v);
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw new IOException(ex.getMessage());
                }
                if (null != analysisf) {
                    rv.put(key, analysisf);
                }
            }

            return true;
        }

        @Override
        public int getCubeDimHigh() {
            int ret = 0;
            for (Field field : fields) {
                // 只有忽略ignore属性字段无需聚合，其他字段都需要聚合
                if (!(field instanceof FieldIgnore)) {
                    ret++;
                }
            }
            return ret;
        }

        @Override
        public List<Integer> getCubeDims() {
            List<Integer> ret = new ArrayList<Integer>(getCubeDimHigh());
            for (int i = fields.length - 1; i >= 0; i--) {
                if (!(fields[i] instanceof FieldIgnore)) {
                    ret.add(i);
                }
            }
            return ret;
        }

        @Override
        public Field[] getFields() {
            return fields;
        }


        /**
         * 获取任务数据数据路径，文件名，任务时间
         */
//        protected void initScriptOper(GroovyObject groovyObject, String inputFile, Date taskTime, Map<String, Object> conf_out)
        protected void initScriptOper(ScriptOper groovyObject, String inputFile, Date taskTime, Map<String, Object> conf_out) {
            ScriptOper scriptOper = (ScriptOper) groovyObject;
            scriptOper.setFilePath(inputFile);

            if (null != inputFile) {
                Path inputFilePath = new Path(inputFile);
                scriptOper.setFileName(inputFilePath.getName());
            }

            scriptOper.setReportDate(taskTime);
            scriptOper.setConfValues(conf_out);
        }

    }

    public static class ReportObjectConf extends ReportObject {
        public ReportObjectConf(AlgDef algd) throws IOException {
            super(algd);
        }

        @Override
        public void parse(Object key, Object value, Configuration conf) {

        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public int getCubeDimHigh() {
            return 0;
        }

        @Override
        public List<Integer> getCubeDims() {
            return null;
        }

        @Override
        public Field[] getFields() {
            return new Field[0];
        }
    }

}
