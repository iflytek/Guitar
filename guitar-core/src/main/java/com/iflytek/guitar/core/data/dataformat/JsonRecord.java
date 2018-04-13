package com.iflytek.guitar.core.data.dataformat;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class JsonRecord extends DataRecord {

    private static final Log LOG = LogFactory.getLog(JsonRecord.class);
    private JSONObject data = null;

    public JsonRecord() {
    }

    public JsonRecord(JSONObject jsonData) {
        this.data = jsonData;
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public DataRecord parseData(Object key, Object value) {
        String jsonString = null;
        if (value instanceof Text) {
            jsonString = value.toString();
        } else if (value instanceof BytesWritable) {
            byte[] b = ((BytesWritable) value).getBytes();
            jsonString = new String(b, 0, ((BytesWritable) value).getLength());
            //jsonString = new String(((BytesWritable)value).getBytes());
        } else {
            LOG.error("record is not instanceof Text/BytesWritable, record type is " + value.getClass().getName());
            return null;
        }

        JSONObject tmpData = null;
        try {
            tmpData = JSONObject.parseObject(jsonString);
        } catch (JSONException e) {
            return null;
        }

        return new JsonRecord(tmpData);
    }

    @Override
    public Object get(String key) throws Exception {
        if (!data.containsKey(key)) {
            return null;
        }

        return data.get(key);
    }

    @Override
    public DataRecord getRecord(String key) throws Exception {
        if (!data.containsKey(key)) {
            return null;
        }

        Object value = data.get(key);
        if (!(value instanceof JSONObject)) {
            throw new Exception("value type of key[" + key + "] is not a DataRecord");
        }

        return new JsonRecord((JSONObject) value);
    }

    @Override
    public Long getLong(String key, Long lDef) throws Exception {
        Long lValue = lDef;
        if (!data.containsKey(key)) {
            return lValue;
        }

        Object value = data.get(key);
        if (value instanceof Long) {
            lValue = (Long) value;
        } else if (value instanceof Integer) {
            lValue = (new Long((Integer) value));
        } else if (value instanceof Boolean) {
            Boolean tmpBool = (Boolean) value;
            lValue = (tmpBool.equals(Boolean.TRUE)) ? 1L : 0L;
        } else {
            try {
                lValue = Long.parseLong(value.toString());
            } catch (Exception e) {
                throw new Exception("value[" + value.toString() + "] not Long type");
            }
        }

        return lValue;
    }

    @Override
    public Double getDouble(String key, Double dDef) throws Exception {
        Double dValue = dDef;
        if (!data.containsKey(key)) {
            return dValue;
        }

        Object value = data.get(key);
        if (value instanceof Double) {
            dValue = (Double) value;
        } else if (value instanceof Float) {
            dValue = new Double((Float) value);
        } else if (value instanceof Long) {
            dValue = new Double((Long) value);
        } else if (value instanceof Integer) {
            dValue = new Double((Integer) value);
        } else if (value instanceof Boolean) {
            Boolean tmpBool = (Boolean) value;
            dValue = (tmpBool.equals(Boolean.TRUE)) ? 1D : 0D;
        } else {
            try {
                dValue = Double.parseDouble(value.toString());
            } catch (Exception e) {
                throw new Exception("value[" + value.toString() + "] not Double type");
            }
        }

        return dValue;
    }

    @Override
    public Boolean getBoolean(String key, Boolean bDef) throws Exception {
        Boolean bValue = bDef;
        Object value = get(key);
        if (null == value) {
            return bValue;
        }

        if (value instanceof Double) {
            bValue = (Boolean) value;
        } else if (value instanceof Float) {
            bValue = (Boolean) value;
        } else if (value instanceof Long) {
            bValue = (Boolean) value;
        } else if (value instanceof Integer) {
            bValue = (Boolean) value;
        } else if (value instanceof Boolean) {
            bValue = (Boolean) value;
        } else {
            try {
                bValue = Boolean.parseBoolean(value.toString());
            } catch (Exception e) {
                throw new Exception("value[" + value.toString() + "] not Boolean type");
            }
        }

        return bValue;

    }

    @Override
    public String getString(String key, String strDef) throws Exception {
        String strValue = strDef;
        if (!data.containsKey(key)) {
            return strValue;
        }

        Object value = data.get(key);
        if (value instanceof String) {
            strValue = (String) value;
        } else {
            strValue = value.toString();
        }

        return strValue;
    }

    @Override
    public Object[] getArray(String key) throws Exception {
        if (!data.containsKey(key)) {
            return null;
        }

        Object value = data.get(key);
        if (!(value instanceof JSONArray)) {
            throw new Exception("value type of key[" + key + "] is not a Array");
        }

        JSONArray jsonArray = (JSONArray) value;
        int len = jsonArray.size();
        Object[] aFeild = new Object[len];
        for (int idx = 0; idx < len; idx++) {
            Object o = jsonArray.get(idx);
            if (o instanceof JSONObject) {
                aFeild[idx] = new JsonRecord((JSONObject) o);
            } else {
                aFeild[idx] = o;
            }
        }

        return aFeild;
    }

}
