package com.iflytek.guitar.core.data.analysisformat;

import com.iflytek.guitar.share.utils.Constants;
import org.apache.avro.reflect.Union;

import java.io.IOException;

/* 指标计算格式基类 */
@Union({SumFormat.class, MaxFormat.class, MinFormat.class, AvgFormat.class, ValueDistFormat.class, RangeDistFormat.class, UVBitmapFormat.class, HyperLogLogFormat.class, HyperLogLogPlusFormat.class})
public abstract class AnalysisFormat extends ObjectValue {
    //public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ObjectValue\",\"namespace\":\"com.iflytek.guitar.util.analysisformat\",\"fields\":[{\"name\":\"value\",\"type\":[\"long\",\"double\"]}]}");

    public static AnalysisFormat getDataFormat(Object value, String analysisType) throws IOException {
        if (Constants.TARGET_FEILD_ANATYPE_SUM.equals(analysisType)) {
            return new SumFormat(value);
        } else if (Constants.TARGET_FEILD_ANATYPE_MAX.equals(analysisType)) {
            return new MaxFormat(value);
        } else if (Constants.TARGET_FEILD_ANATYPE_MIN.equals(analysisType)) {
            return new MinFormat(value);
        } else if (Constants.TARGET_FEILD_ANATYPE_AVG.equals(analysisType)) {
            return new AvgFormat(value);
        } else if (Constants.TARGET_FEILD_ANATYPE_VALUEDIST.equals(analysisType)) {
            return new ValueDistFormat(value);
        } else if (Constants.TARGET_FEILD_ANATYPE_RANGEDIST.equals(analysisType)) {
            return new RangeDistFormat((String)value);
        } else if (Constants.TARGET_FEILD_ANATYPE_UVBITMAP.equals(analysisType)) {
            return new UVBitmapFormat();
        } else if (Constants.TARGET_FEILD_ANATYPE_UV_HYPERLOGLOG.equals(analysisType)) {
            return new HyperLogLogFormat();
        } else if (Constants.TARGET_FEILD_ANATYPE_UV_HYPERLOGLOGPLUS.equals(analysisType)) {
            return new HyperLogLogPlusFormat();
        }
        else {
            throw new IOException("analysisType error: " + analysisType);
        }
    }

    @Override
    public abstract void setValue(Object v);

    public abstract AnalysisFormat merge(AnalysisFormat record) throws IOException;

    public abstract String format() throws IOException;

    public abstract Object toDbValue();

    @Override
    public abstract AnalysisFormat clone();

    @Override
    public abstract String toString();
}
