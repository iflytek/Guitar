package com.iflytek.guitar.core.data.dataformat;

import com.iflytek.guitar.core.alg.Input;

import java.io.IOException;

/* 数据格式基类 */
public abstract class DataRecord {
    public static DataRecord getDataRecord(Input.FileType ft, Input.DataType dt) throws IOException {
        switch (ft) {
            case avro:
                return new AvroRecord();
            case seqfile:
                return new SeqfileRecord();
            case text:
                if (dt == Input.DataType.json) {
                    return new JsonRecord();
                } else {
                    return new TextLineRecord();
                }
            case parquet:
                return new ParquetRecord();
            case orc:
                return new OrcRecord();
            default:
                throw new IOException("Unsupported input type :" + ft + dt);
        }

    }

    public abstract DataRecord parseData(Object key, Object value);

    /* 获取任意类型子字段 */
    public abstract Object get(String key) throws Exception;

    /* 获取record类型子字段 */
    public abstract DataRecord getRecord(String key) throws Exception;

    /* 获取整数类型子字段 */
    public abstract Long getLong(String key, Long lDef) throws Exception;

    /* 获取浮点数类型子字段 */
    public abstract Double getDouble(String key, Double dDef) throws Exception;

    /* 获取布尔类型子字段 */
    public abstract Boolean getBoolean(String key, Boolean bDef) throws Exception;

    /* 获取字符串类型子字段 */
    public abstract String getString(String key, String strDef) throws Exception;

    /* 获取数组类型子字段 */
    public abstract Object[] getArray(String key) throws Exception;

}
