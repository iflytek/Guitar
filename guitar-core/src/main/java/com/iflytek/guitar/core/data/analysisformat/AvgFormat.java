package com.iflytek.guitar.core.data.analysisformat;

import java.io.IOException;

public class AvgFormat extends AnalysisFormat {
    //    public ObjectValue sumValue;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvgFormat\",\"namespace\":\"com.iflytek.guitar.core.data.analysisformat\",\"fields\":[{\"name\":\"value\",\"type\":[\"int\",\"long\",\"double\"]},{\"name\":\"times\",\"type\":\"long\"}]}");

    public Long times;

    public AvgFormat() {

    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    // Used by DatumWriter.  Applications should not call.
    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0:
                return value;
            case 1:
                return times;
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
                times = (Long) value$;
                break;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public AvgFormat(Object data) {
        this.value = data;
        times = 1L;
    }

    @Override
    public void setValue(Object v) {
        value = v;
        times = (long) 1;
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {
        if (record instanceof AvgFormat) {
            AvgFormat that = (AvgFormat) record;
            this.add(that.value);
            this.times += that.times;
        }

        return this;
    }

    @Override
    public String format() throws IOException {
        return this.divided(times).toString();
    }

    @Override
    public String toString() {
        return this.toString() + ":" + times;
    }

    @Override
    public Object toDbValue() {
        try {
            return this.divided(times);
        } catch (IOException ex) {
            ex.printStackTrace();
            return 0.0;
        }
    }

    @Override
    public AnalysisFormat clone() {
        AvgFormat af = new AvgFormat();
        af.times = this.times;
        af.value = this.value;

        return af;
    }
}
