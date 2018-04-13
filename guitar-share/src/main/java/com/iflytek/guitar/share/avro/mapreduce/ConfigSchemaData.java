package com.iflytek.guitar.share.avro.mapreduce;

import com.iflytek.guitar.share.avro.io.UnionData;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
public abstract class ConfigSchemaData extends Configured {
    // public Schema getSchema(Configuration conf);
    public final static String UNION_CLASS = "union.class";
    public final static String UNION_SCHEMA_CUSTOM = "union.schema.custom";

    public static Schema getSchema(Configuration conf) {
        if (null == conf) {
            return null;
        }
        List<Schema> branches = new ArrayList<Schema>();
        branches.add(Schema.create(Schema.Type.NULL));
        Class[] parseClass = null;

        String unionSchemas = conf.get(UNION_SCHEMA_CUSTOM, null);
        if (null != unionSchemas) {
            String[] schemas = unionSchemas.split("@");
            for (String schema : schemas) {
                branches.add(new Schema.Parser().parse(schema));
            }
        } else {
            parseClass = conf.getClasses(UNION_CLASS, null);
            if (parseClass != null) {
                for (Class branch : parseClass) {
                    branches.add(ReflectData.get().getSchema(branch));
                }
            }
        }

        Schema field = Schema.createUnion(branches);
        Schema schema = Schema.createRecord(UnionData.class.getName(), null, null,
                false);
        ArrayList<Field> fields = new ArrayList<Field>();
        fields.add(new Field("datum", field, null, null));
        schema.setFields(fields);
        return schema;
    }
}
