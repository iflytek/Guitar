package com.iflytek.guitar.core.data.analysisformat;

import java.io.IOException;

public class MinFormat extends AnalysisFormat {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MinFormat\",\"namespace\":\"com.iflytek.guitar.core.data.analysisformat\",\"fields\":[{\"name\":\"value\",\"type\":[\"int\",\"long\",\"double\"]}]}");

    //    public ObjectValue value;
    public MinFormat() {

    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return MinFormat.SCHEMA$;
    }

    public MinFormat(Object data) {
        value = data;
    }

    @Override
    public void setValue(Object v) {
        value = v;
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {
        if (record instanceof MinFormat) {
            MinFormat that = (MinFormat) record;
            if (this.compareTo(that) > 0) {
                this.value = that.value;
            }
        }

        return this;
    }

    @Override
    public String format() throws IOException {
        return value.toString();
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public Object toDbValue() {
        return getValue();
    }

    @Override
    public AnalysisFormat clone() {
        return new MinFormat(this.value);
    }
}
