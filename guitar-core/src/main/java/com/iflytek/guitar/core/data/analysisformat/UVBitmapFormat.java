package com.iflytek.guitar.core.data.analysisformat;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.RunningLengthWord;
import com.iflytek.guitar.core.util.javaewah.LongArray;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.IOException;
import java.util.ArrayList;

public class UVBitmapFormat extends AnalysisFormat {
    //    public ObjectValue sumValue;
    //public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"UVBitmapFormat\",\"namespace\":\"com.iflytek.guitar.core.data.analysisformat\",\"fields\":[{\"name\":\"value\",\"type\":[\"int\",\"long\",\"double\"]},{\"name\":\"times\",\"type\":\"long\"}]}");
    public final static org.apache.avro.Schema SCHEMA$;//ReflectDataEx.get().getSchema(UVBitmapFormat.class);
    EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();

    public UVBitmapFormat() {
    }

    static {
        Schema schema_buffer = ReflectData.get().getSchema(LongArray.class);

        Schema shema_rlw = Schema.createRecord(RunningLengthWord.class.getName(), null, RunningLengthWord.class.getPackage().getName(), false);

        ArrayList<Schema.Field> fields_lrw = new ArrayList<Schema.Field>();
        fields_lrw.add(new Schema.Field("buffer", schema_buffer, null, null));
        fields_lrw.add(new Schema.Field("position", ReflectData.get().getSchema(Integer.class), null, null));
        shema_rlw.setFields(fields_lrw);

        Schema schema_bm = Schema.createRecord(EWAHCompressedBitmap.class.getName(), null, EWAHCompressedBitmap.class.getPackage().getName(), false);
        ArrayList<Schema.Field> fields_bm = new ArrayList<Schema.Field>();
        fields_bm.add(new Schema.Field("buffer", schema_buffer, null, null));
        fields_bm.add(new Schema.Field("rlw", shema_rlw, null, null));
        fields_bm.add(new Schema.Field("sizeInBits", ReflectData.get().getSchema(Integer.class), null, null));
        schema_bm.setFields(fields_bm);

        Schema schema = Schema.createRecord(UVBitmapFormat.class.getName(), null, UVBitmapFormat.class.getPackage().getName(), false);
        ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
        fields.add(new Schema.Field("bitmap", schema_bm, null, null));
        schema.setFields(fields);

        SCHEMA$ = schema;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
        return SCHEMA$;
    }
    // Used by DatumWriter.  Applications should not call.

    @Override
    public void setValue(Object v) {
        bitmap.set((Integer) v);
    }

    @Override
    public AnalysisFormat merge(AnalysisFormat record) throws IOException {
        if (record instanceof UVBitmapFormat) {
            UVBitmapFormat that = (UVBitmapFormat) record;
            bitmap = bitmap.or(that.bitmap);
        } else {
            throw new IOException("need UVBitmapFormat but " + record.getClass().getSimpleName());
        }

        return this;
    }

    @Override
    public String format() throws IOException {
        return Integer.toString(bitmap.cardinality());
    }

    @Override
    public String toString() {
        return Integer.toString(bitmap.cardinality());
    }

    @Override
    public Object toDbValue() {
        return bitmap.cardinality();
    }

    /*
     * UVBitmapFormat 忽略value字段，只有bitmap字段，故需要重载该方法
     */
    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0:
                return bitmap;
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
                bitmap = (EWAHCompressedBitmap) value$;
                break;
            default:
                throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public AnalysisFormat clone() {
        UVBitmapFormat uvf = new UVBitmapFormat();
        uvf.value = this.value;

        try {
            uvf.bitmap = this.bitmap.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        return uvf;
    }
}
