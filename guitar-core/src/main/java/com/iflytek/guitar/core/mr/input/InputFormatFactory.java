package com.iflytek.guitar.core.mr.input;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.alg.RunAlgs;
import com.iflytek.guitar.core.util.parquet.SubAvroSchema;
import com.iflytek.guitar.share.avro.mapreduce.input.AvroRecordInputFormat;
import com.iflytek.guitar.share.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

public class InputFormatFactory {

    private static InputFormatFactory iffoctory = null;

    private InputFormatFactory() {
    }

    public static InputFormatFactory get() {
        if (null == iffoctory) {
            iffoctory = new InputFormatFactory();
        }
        return iffoctory;
    }

    public Class get(RunAlgs runalg, AvroJob job) throws ClassNotFoundException {
        Configuration conf = job.getConfiguration();

        String filetype = null;
        String inputClass = null;
        Set<String> parquetFields = new HashSet<String>();

        for (Entry<String, AlgDef> entry : runalg.algs.entrySet()) {
            AlgDef algdef = entry.getValue();
            String file_type = algdef.input.file_type.toString();

            if ("avro".equals(file_type)) {
                return AvroRecordInputFormat.class;
            } else if ("seqfile".equals(file_type)) {
                return SequenceFileInputFormat.class;
            } else if ("text".equals(file_type)) {
                return TextInputFormat.class;
            } else if ("parquet".equals(file_type)) {
                filetype = "parquet";
                if (null != algdef.input.parquet_schema_class) {
                    inputClass = algdef.input.parquet_schema_class;
                }
                if (null != algdef.input.parquet_schema_subfields) {
                    parquetFields.addAll(algdef.input.parquet_schema_subfields);
                }
            } else if ("orc".equals(file_type)) {
                return OrcInputFormat.class;
            } else {
                continue;
            }
        }

        if ("parquet".equals(filetype)) {
            // 若配置读数据所需字段及其类模型信息，则读部分字段
            if (parquetFields != null && parquetFields.size() > 0 && inputClass != null) {
                MessageType schema = SubAvroSchema.getMTByFields(Class.forName(inputClass), parquetFields, conf);
//				conf.set("parquet.avro.projection", schema.toString());
                conf.set("parquet.read.schema", schema.toString());
            }

            conf.set("parquet.avro.data.supplier", "org.apache.parquet.avro.ReflectDataSupplier");
            conf.setBoolean("parquet.avro.compatible", false);

            ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

            return ParquetInputFormat.class;

        } else {
            return null;
        }

    }

}
