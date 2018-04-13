package com.iflytek.guitar.core.data.analysisformat;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ValueDistFormat extends AnalysisFormat {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"ValueDistFormat\",\"namespace\":\"com.iflytek.guitar.core.data.analysisformat\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"map\",\"values\":\"long\",\"avro.java.string\":\"String\"}},{\"name\":\"top\",\"type\":\"int\"}]}");

    public int top;

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0:
                return value;
            case 1:
                return top;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader. Applications should not call.
    @Override
    @SuppressWarnings(value = "unchecked")
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0:
                value = (Map<CharSequence, Long>) value$;
                break;
            case 1:
                top = (Integer) value$;
                break;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return ValueDistFormat.SCHEMA$;
    }

    public ValueDistFormat() {
    }

    public ValueDistFormat(String params) {
        top = Integer.valueOf(params);
    }

    @SuppressWarnings("deprecation")
    public ValueDistFormat(Object data) throws IOException {
        if (value == null) {
            value = new HashMap<String, Long>();
        }
        if (value instanceof Map) {
            Map<String, Long> mapValue = (Map) value;
            String strValue = data.toString();
            mapValue.put(strValue, 1L);
        } else {
            throw new IOException("the value type:" + value.getClass().getName() + "is not Map and do not support in ValueDist.");
        }
    }

    @Override
    public void setValue(Object data) {
        if (value == null) {
            value = new HashMap<String, Long>();
        }
        if (value instanceof Map) {
            Map<String, Long> mapValue = (Map) value;
            String strValue = data.toString();
            mapValue.put(strValue, 1L);
        } else {
            new IOException("the value type:" + value.getClass().getName() + "is not Map and do not support in ValueDist.").printStackTrace();
        }
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {
        if (value instanceof Map) {
            Map<String, Long> mapValue = (Map) value;
            Map<String, Long> thatMapValue = (Map) record.value;

            if (record instanceof ValueDistFormat) {
                for (Entry<String, Long> entry : thatMapValue.entrySet()) {
                    String thatKey = entry.getKey();
                    Long lNum = entry.getValue();
                    if (mapValue.containsKey(thatKey)) {
                        Long tmpValue = mapValue.get(thatKey) + lNum;
                        mapValue.put(thatKey, tmpValue);
                    } else {
                        mapValue.put(thatKey, lNum);
                    }
                }
            }
        } else {
            throw new IOException("the value type:" + value.getClass().getName() + "is not Map and do not support in ValueDist.");
        }

        return this;
    }

    private class StringLong implements Comparable<StringLong> {
        private String name = "";
        private Long count = 0L;

        public StringLong(String name, Long count) {
            super();
            this.name = name;
            this.count = count;
        }

        // 注意这个地方要写成降序
        @Override
        public int compareTo(StringLong o) {
            return o.count.compareTo(this.count);
        }
    }

    @Override
    public String format() throws IOException {
        if (value instanceof Map) {
            Map<String, Long> mapValue = (Map) value;
            // 将记录排序
            List<StringLong> slList = Lists.newArrayList();
            for (Entry<String, Long> entry : mapValue.entrySet()) {
                slList.add(new StringLong(entry.getKey(), entry.getValue()));
            }
            Collections.sort(slList);
            // End Add

            int count = 0;
            // 如果top小于等于零，则相当于没有top
            if (0 >= top)
                // 如此设置count，使得count在循环中不会等于top
            {
                count = top + 1;
            }

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
                ++count;
                if (count == this.top) {
                    break;
                }
            }

            return jo.toString();

        } else {
            throw new IOException("the value type:" + value.getClass().getName() + "is not Map and do not support in ValueDist.");
        }

    }

    @Override
    public String toString() {
        return value.toString();
    }

    public int getTop() {
        return top;
    }

    public void setTop(int top) {
        this.top = top;
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
        ValueDistFormat rdf = new ValueDistFormat();
        rdf.top = this.top;
        Map v_s = (Map) this.value;
        Map v_d = new HashMap();
        for (Object k : v_s.keySet()) {
            v_d.put(k, v_s.get(k));
        }
        rdf.value = v_d;

        return rdf;
    }

}
