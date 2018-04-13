package com.iflytek.guitar.share.avro.io;

import com.iflytek.guitar.share.avro.mapreduce.ConfigSchemaData;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;

@SuppressWarnings("rawtypes")
public class UnionData extends ConfigSchemaData implements Comparable<UnionData> {
    public static final Log LOG = LogFactory.getLog(UnionData.class);

    public static Schema schema = null;
    public Object datum;

    public UnionData(Object datum) {
        this.datum = datum;
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (schema == null) {
            schema = ConfigSchemaData.getSchema(conf);
        }
    }

    public UnionData() {
    }

    public static void setUnionClass(Configuration conf, Class... classez) {
        if (conf == null || classez == null) {
            return;
        }
        ArrayList<String> arrParse = new ArrayList<String>(classez.length);
        for (Class clzss : classez) {
            arrParse.add(clzss.getName());
        }
        conf.setStrings(UNION_CLASS, arrParse.toArray(new String[arrParse.size()]));
    }

    public static void setUnionClass(Configuration conf, Iterable<Class> classez) {
        if (conf == null || classez == null) {
            return;
        }
        ArrayList<String> arrParse = new ArrayList<String>();
        for (Class clzss : classez) {
            arrParse.add(clzss.getName());
        }
        conf.setStrings(UNION_CLASS, arrParse.toArray(new String[arrParse.size()]));
    }

    public static void setUnionSchema(Configuration conf, String strs) {
        if (null == conf || null == strs) {
            return;
        }
        conf.setStrings(UNION_SCHEMA_CUSTOM, strs);
    }

    public static void setParseClass(Job job, Class[] parseClass) {
        if (job == null || parseClass == null) {
            return;
        }
        ArrayList<String> arrParse = new ArrayList<String>(parseClass.length);
        for (Class clzss : parseClass) {
            arrParse.add(clzss.getName());
        }
        job.getConfiguration().setStrings(UNION_CLASS,
                arrParse.toArray(new String[arrParse.size()]));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true; // identical object
        }
        if (!(o instanceof UnionData)) {
            return false; // not a record
        }
        UnionData that = (UnionData) o;
        return ReflectData.get().compare(this, that, schema) == 0;
    }

    @Override
    public int hashCode() {
        if (this.datum == null) {
            return 0;
        }
        return this.datum.hashCode();
    }

    @Override
    public int compareTo(UnionData that) {
        return ReflectData.get().compare(this, that, schema);
    }
}
