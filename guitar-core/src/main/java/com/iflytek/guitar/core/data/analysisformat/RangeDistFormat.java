package com.iflytek.guitar.core.data.analysisformat;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;
import org.jfree.util.Log;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RangeDistFormat extends AnalysisFormat {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"RangeDistFormat\",\"namespace\":\"com.iflytek.guitar.core.data.analysisformat\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"map\",\"values\":\"long\",\"avro.java.string\":\"String\"}},{\"name\":\"range\",\"type\":\"string\"}]}");
    public String range = null;
    public String param = null;

    public RangeDistFormat() {

    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return RangeDistFormat.SCHEMA$;
    }

    // Used by DatumWriter.  Applications should not call.
    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0:
                return value;
            case 1:
                return range;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader.  Applications should not call.
    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0:
                value = (Object) value$;
                break;
            case 1:
                range = (String) value$;
                break;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public RangeDistFormat(String ranParam) {
        this.param = ranParam;
        double min = Double.parseDouble(ranParam.substring(0, ranParam.indexOf("~")));
        double interm = Double.parseDouble(ranParam.substring(ranParam.indexOf("~") + 1, ranParam.indexOf("~", ranParam.indexOf("~") + 1)));
        double max = Double.parseDouble(ranParam.substring(ranParam.indexOf("~", ranParam.indexOf("~") + 1) + 1, ranParam.length()));
        range = "min:" + min + ",interval:" + interm + ",max:" + max;
    }

    public RangeDistFormat(Object v, String ranParam) throws IOException {
        //分别取出用户定义的范围区间和间隔min~interm~max
        double min = Double.parseDouble(ranParam.substring(0, ranParam.indexOf("~")));
        double interm = Double.parseDouble(ranParam.substring(ranParam.indexOf("~") + 1, ranParam.indexOf("~", ranParam.indexOf("~") + 1)));
        double max = Double.parseDouble(ranParam.substring(ranParam.indexOf("~", ranParam.indexOf("~") + 1) + 1, ranParam.length()));
        range = "min:" + min + ",interval:" + interm + ",max:" + max;
        int length = (int) ((max - min) / interm) + 1;
        if (value == null) {
            value = new HashMap<String, Long>();
        }
        if (value instanceof Map) {
            Map mapValue = (Map) value;
            try {
                double tmpvalue = Double.parseDouble(v.toString());
                if (tmpvalue < min) {
                    (mapValue).put(0, 1L);
                } else if (tmpvalue > max) {
                    (mapValue).put(length, 1L);
                } else {
                    int i = (int) (tmpvalue / interm) + 1;
                    (mapValue).put(i, 1L);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else {
            throw new IOException("the value type:" + value.getClass().getName() + "is not Map and do not support in Rangedist.");
        }

    }

    @Override
    public void setValue(Object v) {
        //分别取出用户定义的范围区间和间隔min~interm~max
        double min = Double.parseDouble(param.substring(0, param.indexOf("~")));
        double interm = Double.parseDouble(param.substring(param.indexOf("~") + 1, param.indexOf("~", param.indexOf("~") + 1)));
        double max = Double.parseDouble(param.substring(param.indexOf("~", param.indexOf("~") + 1) + 1, param.length()));
        int length = (int) ((max - min) / interm) + 1;
        if (value == null) {
            value = new HashMap<String, Long>();
        }
        if (value instanceof Map) {
            Map mapValue = (Map) value;
            try {
                double tmpvalue = Double.parseDouble(v.toString());
                if (tmpvalue < min) {
                    (mapValue).put(0, 1L);
                } else if (tmpvalue > max) {
                    (mapValue).put(length, 1L);
                } else {
                    int i = (int) (tmpvalue / interm) + 1;
                    (mapValue).put(i, 1L);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else {
            Log.error("the value type:" + value.getClass().getName() + "is not Map and do not support in Rangedist.");
        }
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {
        if (value instanceof Map) {
            Map<String, Long> mapValue = (Map) value;
            Map<String, Long> thatMapValue = (Map) record.value;

            if (record instanceof RangeDistFormat) {
                for (Entry<String, Long> entry : thatMapValue.entrySet()) {
                    String thatKey = (String) entry.getKey();
                    Long lNum = (Long) entry.getValue();
                    if (mapValue.containsKey(thatKey)) {
                        Long tmpValue = mapValue.get(thatKey) + lNum;
                        mapValue.put(thatKey, tmpValue);
                    } else {
                        mapValue.put(thatKey, lNum);
                    }
                }

            }
        } else {
            throw new IOException("the value type:" + value.getClass().getName() + "is not Map and do not support in Rangedist.");
        }
        return this;
    }

    @Override
    public String format() throws IOException {
        if (value instanceof Map) {
            Map<String, Long> mapValue = (Map) value;

//			return range.toString() + "," + mapValue.toString();
            //重写hashmap按key排序的toString方法
            // 将记录排序
            List<StringLong> slList = Lists.newArrayList();
            for (Entry<String, Long> entry : mapValue.entrySet()) {
                slList.add(new StringLong(entry.getKey(), entry.getValue()));
            }
            Collections.sort(slList);

			/* 转换为json格式串 */
            JSONObject jo = new JSONObject();
            for (StringLong sl : slList) {
                String key = sl.name;
                long value = sl.count;

                try {
                    jo.put(key, value);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
            return range.toString() + "," + jo.toString();
        } else {
            throw new IOException("the value type:" + value.getClass().getName() + "is not Map and do not support in Rangedist.");
        }
    }

    private class StringLong implements Comparable<StringLong> {
        private String name = "";
        private Long count = 0L;

        public StringLong(String name, Long count) {
            super();
            this.name = name;
            this.count = count;
        }

        // 按照name排序
        @Override
        public int compareTo(StringLong o) {
            Long thisValue = Long.parseLong(this.name);
            Long thatValue = Long.parseLong(o.name);
            return thisValue.compareTo(thatValue);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public String toString() {
        return value.toString();
    }

    @Override
    public Object toDbValue() {
        try {
            return format();
        } catch (IOException ex) {
            ex.printStackTrace();
            return "";
        }
    }

    @Override
    public AnalysisFormat clone() {
        RangeDistFormat rdf = new RangeDistFormat();
        rdf.range = this.range;
        rdf.param = this.param;
        Map v_s = (Map) this.value;
        Map v_d = new HashMap();
        for (Object k : v_s.keySet()) {
            v_d.put(k, v_s.get(k));
        }
        rdf.value = v_d;

        return rdf;
    }
}
