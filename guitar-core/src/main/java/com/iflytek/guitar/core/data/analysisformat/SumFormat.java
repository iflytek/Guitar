package com.iflytek.guitar.core.data.analysisformat;

import java.io.IOException;

public class SumFormat extends AnalysisFormat {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"SumFormat\",\"namespace\":\"com.iflytek.guitar.core.data.analysisformat\",\"fields\":[{\"name\":\"value\",\"type\":[\"int\",\"long\",\"double\"]}]}");

    public SumFormat() {

    }

    public SumFormat(Object num) {
        value = num;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return SumFormat.SCHEMA$;
    }

    @Override
    public void setValue(Object v) {
        value = v;
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {
        if (record instanceof SumFormat) {
            this.add(record.value);
        }

        return this;
    }

    @Override
    public String format() throws IOException {
        return value.toString();
    }

    @Override
    public String toString() {
        return "SUM(" + value.toString() + ")";
    }

    @Override
    public Object toDbValue() {
        return getValue();
    }

    @Override
    public AnalysisFormat clone() {
        return new SumFormat(this.value);
    }
}
