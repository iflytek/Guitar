package com.iflytek.guitar.core.data.analysisformat;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.util.hyperloglog.HyperLogLogPlus;
import com.iflytek.guitar.share.avro.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;

import java.io.IOException;
import java.util.ArrayList;

public class HyperLogLogPlusFormat extends AnalysisFormat {
    public static org.apache.avro.Schema SCHEMA$;

	/*
     * public static Field fieldsm; public static Field fieldsp; public static
	 * Field fieldformat; public static Field fieldsparseSetThreshold; public
	 * static Field fieldtmpIndex; public static Field fieldtmpSet; public
	 * static Field fieldsparseSet; public static Field fieldregisterset;
	 * 
	 * public static Field fieldregistersetCount; public static Field
	 * fieldregistersetM;
	 */

    static {
        Schema hllpSchema = ReflectData.get().getSchema(HyperLogLogPlus.class);

        Schema schema = Schema.createRecord(
                HyperLogLogPlusFormat.class.getName(), null,
                HyperLogLogPlusFormat.class.getPackage().getName(), false);
        ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
        fields.add(new Schema.Field("hllp", hllpSchema, null, null));
        schema.setFields(fields);

        SCHEMA$ = schema;
    }

    @Nullable
    public HyperLogLogPlus hllp;

    @Override
    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }

    /*
     * HyperLogLogPlusFormat 忽略value字段，只有hllp字段，故需要重载该方法
     */
    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0:
                return hllp;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader. Applications should not call.
    @SuppressWarnings(value = "unchecked")
    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0:
                hllp = (HyperLogLogPlus) value$;
                break;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public HyperLogLogPlusFormat() {
        hllp = new HyperLogLogPlus(14);
    }

    public HyperLogLogPlusFormat(AlgDef.Frequence mainFreq) {
        if (mainFreq == AlgDef.Frequence.Hourly) {
            hllp = new HyperLogLogPlus(14, 14);
        } else {
            hllp = new HyperLogLogPlus(14);
        }
    }

    @Override
    public void setValue(Object v) {
        hllp.offer(v);
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {
        try {
            if (record instanceof HyperLogLogPlusFormat) {
                HyperLogLogPlusFormat that = (HyperLogLogPlusFormat) record;
                hllp.addAll(that.hllp);
            }
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }

        return this;
    }

    @Override
    public String format() throws IOException {
        return Long.toString(hllp.cardinality());
    }

    @Override
    public String toString() {
        return Long.toString(hllp.cardinality());
    }

    @Override
    public Object toDbValue() {
        return hllp.cardinality();
    }

    @Override
    public AnalysisFormat clone() {
        HyperLogLogPlusFormat hllpf = new HyperLogLogPlusFormat();

        try {
            hllpf.hllp = AvroUtils.clone(this.hllp);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hllpf;
    }
}
