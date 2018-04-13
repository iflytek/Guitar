package com.iflytek.guitar.core.data.dataformat;

import com.iflytek.guitar.share.avro.io.Pair;
import com.iflytek.guitar.share.avro.reflect.ReflectDataEx;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;

import java.util.List;
import java.util.Map;

public class AvroRecord extends DataRecord {
    private static enum DataType {
        TYPE_PAIR,
        TYPE_RECORD,
        TYPE_FEILD,
        TYPE_ARRAY
    }

    Object data = null;

    public AvroRecord() {
    }

    public AvroRecord(Object value) {
        data = value;
    }

    private DataType type(Object value) {
        if (value instanceof Pair) {
            return DataType.TYPE_PAIR;
        } else if (value instanceof GenericData.Record || value instanceof Map) {
            return DataType.TYPE_RECORD;
        } else if (value instanceof List) /* avro中的array和GenericArray都继承于List */ {
            return DataType.TYPE_ARRAY;
        }
        return DataType.TYPE_FEILD;
    }

    private Object _get(String key) throws Exception {
        if (null == key) {
            return null;
        }

        DataType type = type(data);
        Object value = null;

        if (type == DataType.TYPE_PAIR) {
            /* 对于pair类型来说, "key"表示0索引位置, "value"为1索引位置  */
            Pair dataPair = (Pair) data;
            if ("key".equals(key)) {
                value = dataPair.get(0);
            } else if ("value".equals(key)) {
                value = dataPair.get(1);
            } else {
                return null;
            }
        } else if (type == DataType.TYPE_RECORD) {
            if (data instanceof GenericData.Record) {
                GenericData.Record recordData = (GenericData.Record) data;
                value = recordData.get(key);
            } else if (data instanceof Map) {
                Map mapData = (Map) data;
                value = mapData.get(key);
            } else {
                throw new Exception("data's class[" + data.getClass().getName() + "] invalid, where its type is RECORD");
            }
        } else {
            throw new Exception("data's type[" + type.toString() + "] invalid and its's class is " + data.getClass().getName() + ", cannot user _get interface");
        }

        return value;
    }

    private Object[] _toArray(List avroArray) throws Exception {
        if (null == avroArray) {
            return null;
        }

        if (0 == avroArray.size()) {
            return new Object[0];
        }

        Object[] aFeild = new Object[avroArray.size()];
        if (DataType.TYPE_RECORD == type(avroArray.get(0))) {
            for (int idx = 0; idx < avroArray.size(); idx++) {
                Object o = avroArray.get(idx);
                if (DataType.TYPE_RECORD != type(o)) {
                    throw new Exception("value type of idx[" + idx + "] in array is not a DataRecord, toString is " + o.toString());
                }
                aFeild[idx] = new AvroRecord(o);
            }
        } else {
            for (int idx = 0; idx < avroArray.size(); idx++) {
                Object o = avroArray.get(idx);
                if (DataType.TYPE_FEILD != type(o)) {
                    throw new Exception("value type of idx[" + idx + "] in array is not a FEILD, toString is " + o.toString());
                }
                aFeild[idx] = o;
            }
        }

        return aFeild;
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public DataRecord parseData(Object key, Object value) {
        if (null == key) {
            key = "null";
        }

        if (null == value) {
            return null;
        }

        Schema schKey = null;
        if (key instanceof GenericContainer) {
            schKey = ((GenericContainer) key).getSchema();
        } else {
            schKey = ReflectDataEx.get().getSchema(key.getClass());
        }

        Schema schValue = null;
        if (value instanceof GenericContainer) {
            schValue = ((GenericContainer) value).getSchema();
        } else {
            schValue = ReflectDataEx.get().getSchema(value.getClass());
        }

        Pair<Object, Object> pair = new Pair(key, schKey, value, schValue);

        AvroRecord record = new AvroRecord(pair);

        return record;
    }

    @Override
    public Object get(String key) throws Exception {
        if (null == key) {
            return null;
        }

        Object value = _get(key);
        if (null == value) {
            return null;
        }

        DataType type = type(value);
        if (DataType.TYPE_RECORD == type || DataType.TYPE_PAIR == type) {
            return new AvroRecord(value);
        } else if (DataType.TYPE_ARRAY == type) {
            return _toArray((List) value);
        } else {
            return value;
        }
    }

    @Override
    public DataRecord getRecord(String key) throws Exception {
        Object value = get(key);
        if (null == value) {
            return null;
        }

        if (!(value instanceof AvroRecord)) {
            throw new Exception("value's type[" + value.getClass().getName() + "] is not RECORD, where key=" + key);
        }

        return (DataRecord) value;
    }

    @Override
    public Long getLong(String key, Long lDef) throws Exception {
        Object value = get(key);
        Long lValue = lDef;

        if (null == value) {
            return lValue;
        }

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
        Object value = get(key);
        Double dValue = dDef;
        if (null == value) {
            return dValue;
        }

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
        Object value = get(key);
        Boolean bValue = bDef;
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
        Object value = get(key);
        String strValue = strDef;
        if (null == value) {
            return strValue;
        }

        if (value instanceof String) {
            strValue = (String) value;
        } else {
            strValue = value.toString();
        }

        return strValue;
    }

    @Override
    public Object[] getArray(String key) throws Exception {
        Object value = _get(key);
        if (null == value) {
            return null;
        }

        if (DataType.TYPE_ARRAY != type(value)) {
            throw new Exception("value's type[" + value.getClass().getName() + "] is not ARRAY, where key=" + key);
        }

        return _toArray((List) value);
    }

}
