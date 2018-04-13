package com.iflytek.guitar.core.data.dataformat;

import org.apache.parquet.example.data.Group;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetRecord extends DataRecord {

    //    private Object data = null;
    public Object data = null;

    public ParquetRecord() {
    }

    public ParquetRecord(Object value) {
        data = value;
    }

    @Override
    public DataRecord parseData(Object key, Object value) {
        ParquetRecord record = new ParquetRecord(value);
        return record;
    }

    @Override
    public Object get(String key) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            Group tmp = group.getGroup(key, 0);
            return new ParquetRecord(tmp);
        }
        return null;
    }

    @Override
    public DataRecord getRecord(String key) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            Group tmp = group.getGroup(key, 0);
            return new ParquetRecord(tmp);
        }
        return null;
    }

    public Integer getInt(String key, int iDef) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            Integer value = group.getInteger(key, 0);
            if (value != null && value != Integer.MIN_VALUE) {
                return value;
            }
        }
        return iDef;
    }

    @Override
    public Long getLong(String key, Long lDef) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            Long value = group.getLong(key, 0);
            if (value != null && value != Long.MIN_VALUE) {
                return value;
            }
        }
        return lDef;
    }

    public Float getFloat(String key, float fDef) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            Float value = group.getFloat(key, 0);
            if (value != null && value != Float.MIN_VALUE) {
                return value;
            }
        }
        return fDef;
    }

    @Override
    public Double getDouble(String key, Double dDef) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            Double value = group.getDouble(key, 0);
            if (value != null && value != Double.MIN_VALUE) {
                return value;
            }
        }
        return dDef;
    }

    @Override
    public Boolean getBoolean(String key, Boolean bDef) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            Boolean value = group.getBoolean(key, 0);
            if (value != null) {
                return value;
            }
        }
        return bDef;
    }

    @Override
    public String getString(String key, String strDef) throws Exception {
        if (data instanceof Group) {
            Group group = (Group) data;
            String value = group.getString(key, 0);
            if (value != null && value.length() != 0) {
                return value;
            }
        }
        return strDef;
    }

    @Override
    public Object[] getArray(String key) throws Exception {
        throw new Exception("interface getArray not used in ParquetRecord");
    }

    /**
     * key: 字段名
     * dataType: 数据类型，Integer、Long、Float、Double、Boolean、String及DataRecord
     */
    public List<Object> getList(String key, String dataType) throws Exception {
        List<Object> list = null;

        // 先取字段，其值塞到DataRecord的data里
        ParquetRecord record = (ParquetRecord) getRecord(key);

        if (record.data instanceof Group) {
            list = new ArrayList<Object>();
            Group group = (Group) record.data;
            // parquet 旧List两层结构，有多个名为array的字段，每个array字段里存放实际值
            int arraySize = group.getFieldRepetitionCount("array");
            for (int i = 0; i < arraySize; i++) {
                if ("Integer".equals(dataType)) {
                    Integer element = group.getInteger("array", i);
                    list.add(element);
                } else if ("Long".equals(dataType)) {
                    Long element = group.getLong("array", i);
                    list.add(element);
                } else if ("Float".equals(dataType)) {
                    Float element = group.getFloat("array", i);
                    list.add(element);
                } else if ("Double".equals(dataType)) {
                    Double element = group.getDouble("array", i);
                    list.add(element);
                } else if ("Boolean".equals(dataType)) {
                    Boolean element = group.getBoolean("array", i);
                    list.add(element);
                } else if ("String".equals(dataType)) {
                    String element = group.getString("array", i);
                    list.add(element);
                } else if ("DataRecord".equals(dataType)) {
                    Group element = group.getGroup("array", i);
                    list.add(new ParquetRecord(element));
                } else {
                    throw new Exception("dataType: " + dataType + " not support in ParquetRecord");
                }
            }
            // parquet 新List两层结构，有多个名为list的字段，每个list字段里包含一个名为element的字段，存放实际值
            int listSize = group.getFieldRepetitionCount("list");
            for (int i = 0; i < listSize; i++) {
                Group listGroup = group.getGroup("list", i);
                if ("Integer".equals(dataType)) {
                    Integer element = listGroup.getInteger("element", 0);
                    list.add(element);
                } else if ("Long".equals(dataType)) {
                    Long element = listGroup.getLong("element", 0);
                    list.add(element);
                } else if ("Float".equals(dataType)) {
                    Float element = listGroup.getFloat("element", 0);
                    list.add(element);
                } else if ("Double".equals(dataType)) {
                    Double element = listGroup.getDouble("element", 0);
                    list.add(element);
                } else if ("Boolean".equals(dataType)) {
                    Boolean element = listGroup.getBoolean("element", 0);
                    list.add(element);
                } else if ("String".equals(dataType)) {
                    String element = listGroup.getString("element", 0);
                    list.add(element);
                } else if ("DataRecord".equals(dataType)) {
                    list.add(new ParquetRecord(listGroup));
                } else {
                    throw new Exception("dataType: " + dataType + " not support in ParquetRecord");
                }
            }
        }

        return list;
    }

    /**
     * key: 字段名
     * dataType: value的数据类型，Integer、Long、Float、Double、Boolean、String及DataRecord
     */
    public Map<String, Object> getMap(String key, String dataType) throws Exception {
        Map<String, Object> map = null;

        // 先取字段，其值塞到DataRecord的data里
        ParquetRecord record = (ParquetRecord) getRecord(key);

        // parquet Map两层结构，有多个名为map的字段，每个map字段里包含一个名为key的字段和一个名为value的字段，存放实际kv值
        if (record.data instanceof Group) {
            map = new HashMap<String, Object>();
            Group group = (Group) record.data;
            int size = group.getFieldRepetitionCount("map");
            for (int i = 0; i < size; i++) {
                Group mapGroup = group.getGroup("map", i);
                if ("Integer".equals(dataType)) {
                    String k = mapGroup.getString("key", 0);
                    Integer v = mapGroup.getInteger("value", 0);
                    map.put(k, v);
                } else if ("Long".equals(dataType)) {
                    String k = mapGroup.getString("key", 0);
                    Long v = mapGroup.getLong("value", 0);
                    map.put(k, v);
                } else if ("Float".equals(dataType)) {
                    String k = mapGroup.getString("key", 0);
                    Float v = mapGroup.getFloat("value", 0);
                    map.put(k, v);
                } else if ("Double".equals(dataType)) {
                    String k = mapGroup.getString("key", 0);
                    Double v = mapGroup.getDouble("value", 0);
                    map.put(k, v);
                } else if ("Boolean".equals(dataType)) {
                    String k = mapGroup.getString("key", 0);
                    Boolean v = mapGroup.getBoolean("value", 0);
                    map.put(k, v);
                } else if ("String".equals(dataType)) {
                    String k = mapGroup.getString("key", 0);
                    String v = mapGroup.getString("value", 0);
                    map.put(k, v);
                } else if ("DataRecord".equals(dataType)) {
                    String k = mapGroup.getString("key", 0);
                    Group v = mapGroup.getGroup("value", 0);
                    map.put(k, new ParquetRecord(v));
                } else {
                    throw new Exception("dataType: " + dataType + " not support in ParquetRecord");
                }
            }
        }

        return map;
    }

}
