package com.iflytek.guitar.core.data.dataformat;

import org.apache.hadoop.io.BytesWritable;

public class SeqfileRecord extends DataRecord {

    private String data = null;

    public SeqfileRecord() {
    }

    public SeqfileRecord(String seqfileString) {
        this.data = seqfileString;
    }

    @Override
    public String toString() {
        return data;
    }

    @Override
    public DataRecord parseData(Object key, Object value) {
        if (null == value) {
            return null;
        }

        String str = null;
        if (value instanceof BytesWritable) {
            str = new String(((BytesWritable) value).getBytes());
        } else {
            str = value.toString();
        }

        return new SeqfileRecord(str);
    }

    @Override
    public Object get(String key) throws Exception {
        throw new Exception("interface get not used in SeqfileRecord");
    }

    @Override
    public DataRecord getRecord(String key) throws Exception {
        throw new Exception("interface getRecord not used in SeqfileRecord");
    }

    @Override
    public Long getLong(String key, Long lDef) throws Exception {
        throw new Exception("interface getLong not used in SeqfileRecord");
    }

    @Override
    public Double getDouble(String key, Double dDef) throws Exception {
        throw new Exception("interface getDouble not used in SeqfileRecord");
    }

    @Override
    public Boolean getBoolean(String key, Boolean bDef) throws Exception {
        throw new Exception("interface getBoolean not used in SeqfileRecord");
    }

    @Override
    public String getString(String key, String strDef) throws Exception {
        return data;
    }

    @Override
    public Object[] getArray(String key) throws Exception {
        throw new Exception("interface getArray not used in SeqfileRecord");
    }

    public String getData() throws Exception {
        return data;
    }

}
