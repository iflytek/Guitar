package com.iflytek.guitar.core.data;

import com.iflytek.guitar.core.data.analysisformat.AnalysisFormat;
import com.iflytek.guitar.core.data.analysisformat.UVBitmapFormat;
import com.iflytek.guitar.share.db.Exportable;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ReportValue implements Exportable {
    public Map<String, AnalysisFormat> analysisValue = Maps.newHashMap();

    public ReportValue() {
    }

    public ReportValue(Map<String, AnalysisFormat> value) {
        analysisValue = value;
    }

    public void put(String key, AnalysisFormat anaData) {
        analysisValue.put(key, anaData);
    }

    public AnalysisFormat get(String key) {
        return analysisValue.get(key);
    }

    @Override
    public String toString() {
        return analysisValue.toString();
    }

    public void merge(ReportValue rv) throws IOException {
        for (Entry<String, AnalysisFormat> entry : rv.analysisValue.entrySet()) {
            AnalysisFormat af = analysisValue.get(entry.getKey());
            if (null == af) {
                analysisValue.put(entry.getKey(), entry.getValue().clone());
            } else {
                af.merge(entry.getValue());
                //analysisValue.put(entry.getKey(), entry.getValue().merge(af));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ReportValue rv = new ReportValue();
        UVBitmapFormat uvft = new UVBitmapFormat();
        uvft.setValue(4);
        rv.put("mykey", uvft);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Schema schema = ReflectData.get().getSchema(ReportValue.class);
        System.out.println(schema);
        //DatumWriter可以将GenericRecord变成edncoder可以理解的类型
        DatumWriter<ReportValue> writer = new ReflectDatumWriter<ReportValue>(schema);
        //encoder可以将数据写入流中，binaryEncoder第二个参数是重用的encoder，这里不重用，所用传空
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(rv, encoder);
        encoder.flush();
        out.close();
    }

    @Override
    public Map<String, String> getExportFeilds() {
        Map<String, String> ret = new HashMap<String, String>();
        for (Entry<String, AnalysisFormat> entry : analysisValue.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().toDbValue().toString());
        }

        return ret;
    }
}
