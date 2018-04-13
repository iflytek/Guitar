package com.iflytek.guitar.core.data.analysisformat;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.iflytek.guitar.core.util.hyperloglog.HyperLogLog;
import com.iflytek.guitar.core.util.hyperloglog.RegisterSet;
import org.apache.avro.Schema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;

import java.io.IOException;
import java.util.ArrayList;

public class HyperLogLogFormat extends AnalysisFormat {

    public static org.apache.avro.Schema SCHEMA$;

    static {
        Schema hllSchema = ReflectData.get().getSchema(HyperLogLog.class);

        Schema schema = Schema.createRecord(HyperLogLogFormat.class.getName(),
                null, HyperLogLogFormat.class.getPackage().getName(), false);
        ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
        fields.add(new Schema.Field("hll", hllSchema, null, null));
        schema.setFields(fields);

        SCHEMA$ = schema;
    }

    @Nullable
    public HyperLogLog hll = new HyperLogLog(0.01);

    @Override
    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    /*
     * HyperLogLogFormat 忽略value字段，只有hll字段，故需要重载该方法
     */
    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0:
                return hll;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value = "unchecked")
    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0:
                hll = (HyperLogLog) value$;
                break;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }


    public HyperLogLogFormat() {
        // hll = new HyperLogLog(0.01);
    }

    @Override
    public void setValue(Object v) {
        hll.offer(v);
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {

        try {
            if (record instanceof HyperLogLogFormat) {
                HyperLogLogFormat that = (HyperLogLogFormat) record;
                hll.addAll(that.hll);
            }
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }

        return this;
    }

    @Override
    public String format() throws IOException {
        return Long.toString(hll.cardinality());
    }

    @Override
    public String toString() {
        return Long.toString(hll.cardinality());
    }

    @Override
    public Object toDbValue() {
        return hll.cardinality();
    }

    @Override
    public AnalysisFormat clone() {
        HyperLogLogFormat hllf = new HyperLogLogFormat();
        hllf.hll.registerSet = new RegisterSet();
        hllf.hll.registerSet.count = this.hll.registerSet.count;
        hllf.hll.registerSet.size = this.hll.registerSet.size;
        hllf.hll.registerSet.M = this.hll.registerSet.M.clone();

        hllf.hll.alphaMM = this.hll.alphaMM;
        hllf.hll.log2m = this.hll.log2m;

        hllf.value = this.value;

        return hllf;
    }

}
