package com.iflytek.guitar.core.data.dataformat;

import org.apache.hadoop.io.*;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OrcRecord extends DataRecord {

    //    private Object data = null;
    public Object data = null;

    public OrcRecord() {
    }

    public OrcRecord(Object value) {
        data = value;
    }

    @Override
    public DataRecord parseData(Object key, Object value) {
        OrcRecord record = new OrcRecord(value);
        return record;
    }

    @Override
    public Object get(String key) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            OrcStruct tmp = (OrcStruct) orcStruct.getFieldValue(key);
            if (tmp != null) {
                return new OrcRecord(tmp);
            }
        }
        return null;
    }

    @Override
    public DataRecord getRecord(String key) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            OrcStruct tmp = (OrcStruct) orcStruct.getFieldValue(key);
            if (tmp != null) {
                return new OrcRecord(tmp);
            }
        }
        return null;
    }

    public Integer getInt(String key, int iDef) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            WritableComparable fieldValue = orcStruct.getFieldValue(key);
            if (fieldValue != null) {
                Integer value = ((IntWritable) fieldValue).get();
                return value;
            }
        }
        return iDef;
    }

    @Override
    public Long getLong(String key, Long lDef) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            WritableComparable fieldValue = orcStruct.getFieldValue(key);
            if (fieldValue != null) {
                Long value = ((LongWritable) fieldValue).get();
                return value;
            }
        }
        return lDef;
    }

    public Float getFloat(String key, float fDef) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            WritableComparable fieldValue = orcStruct.getFieldValue(key);
            if (fieldValue != null) {
                Float value = ((FloatWritable) fieldValue).get();
                return value;
            }
        }
        return fDef;
    }

    @Override
    public Double getDouble(String key, Double dDef) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            WritableComparable fieldValue = orcStruct.getFieldValue(key);
            if (fieldValue != null) {
                Double value = ((DoubleWritable) fieldValue).get();
                return value;
            }
        }
        return dDef;
    }

    @Override
    public Boolean getBoolean(String key, Boolean bDef) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            WritableComparable fieldValue = orcStruct.getFieldValue(key);
            if (fieldValue != null) {
                Boolean value = ((BooleanWritable) fieldValue).get();
                return value;
            }
        }
        return bDef;
    }

    @Override
    public String getString(String key, String strDef) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            WritableComparable fieldValue = orcStruct.getFieldValue(key);
            if (fieldValue != null) {
                String value = ((Text) fieldValue).toString();
                return value;
            }
        }
        return strDef;
    }

    @Override
    public Object[] getArray(String key) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            WritableComparable fieldValue = orcStruct.getFieldValue(key);
            if (fieldValue != null) {
                Object[] value = ((OrcList) fieldValue).toArray();
                if (value.length > 0) {
                    if (!(value[0] instanceof OrcStruct)) {
                        return value;
                    } else {
                        DataRecord[] dataRecords = new DataRecord[value.length];
                        for (int i = 0; i < value.length; i++) {
                            dataRecords[i] = new OrcRecord(value[i]);
                        }
                        return dataRecords;
                    }
                }
            }
        }
        return null;
    }

    public Map<String, Object> getMap(String key) throws Exception {
        if (data instanceof OrcStruct) {
            OrcStruct orcStruct = (OrcStruct) data;
            OrcMap value = (OrcMap) orcStruct.getFieldValue(key);
            if (value != null && value.size() > 0) {
                Map<String, Object> map = new HashMap<String, Object>();
                Set<Map.Entry<Object, Object>> set = value.entrySet();
                for (Map.Entry<Object, Object> entry : set) {
                    if (entry.getValue() instanceof OrcStruct) {
                        map.put(((Text) entry.getKey()).toString(), new OrcRecord(entry.getValue()));
                    } else {
                        map.put(((Text) entry.getKey()).toString(), entry.getValue());
                    }
                }
                return map;
            }
        }
        return null;
    }

}
