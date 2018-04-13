package com.iflytek.guitar.share.avro.mapreduce;

import com.iflytek.guitar.share.avro.io.AvroKeyComparator;
import com.iflytek.guitar.share.avro.serializer.AvroReflectDXSerialization;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapred.InputFormat} for sequence files.
 */
public class AvroJob extends Job {
    public static final String MAP_OUTPUT_SCHEMA = "avro.map.output.schema";

    public AvroJob() throws IOException {
        this(new Configuration());
        initAvro();
    }

    public AvroJob(Configuration conf) throws IOException {
        super(conf);
        initAvro();
    }

    public AvroJob(Configuration conf, String jobName) throws IOException {
        this(conf);
        setJobName(jobName);
        initAvro();
    }

    @SuppressWarnings("deprecation")
    private void initAvro() {
        this.setJarByClass(AvroJob.class);
        this.setSortComparatorClass(AvroKeyComparator.class);
        this.getConfiguration().setStrings(
                "io.serializations",
                new String[]{WritableSerialization.class.getName(),
                        AvroReflectDXSerialization.class.getName()});
        this.conf.setCompressMapOutput(true);
    }

    public static AvroJob getAvroJob(Configuration conf) throws IOException {
        AvroJob job = new AvroJob(conf);
        //job.setSpeculativeExecution(true);
        return job;
    }

    @Override
    public void setSpeculativeExecution(boolean speculativeExecution) {
        setMapSpeculativeExecution(speculativeExecution);
        setReduceSpeculativeExecution(speculativeExecution);
    }

    public boolean getMapSpeculativeExecution() {
        return this.conf.getBoolean("mapred.map.tasks.speculative.execution", true);
    }

    @Override
    public void setMapSpeculativeExecution(boolean speculativeExecution) {
        this.conf.setBoolean("mapred.map.tasks.speculative.execution",
                speculativeExecution);
    }

    public boolean getReduceSpeculativeExecution() {
        return this.conf.getBoolean("mapred.reduce.tasks.speculative.execution",
                true);
    }

    @Override
    public void setReduceSpeculativeExecution(boolean speculativeExecution) {
        this.conf.setBoolean("mapred.reduce.tasks.speculative.execution",
                speculativeExecution);
    }

    public static void setMapOutputSchema(Configuration conf, Schema schema) {
        conf.set(MAP_OUTPUT_SCHEMA, schema.toString());
    }

    public static Schema getMapOutputSchema(Configuration conf) {
        if (conf.get(MAP_OUTPUT_SCHEMA) == null) {
            return null;
        }
        return Schema.parse(conf.get(MAP_OUTPUT_SCHEMA));

    }

    public void setMapOutputValueSchema(Schema schema) {
        this.conf.set("avro.map.output.value.schema", schema.toString());
    }

    public Schema getMapOutputValueSchema() {
        if (this.conf.get("avro.map.output.value.schema") == null) {
            return null;
        }
        return new Schema.Parser().parse(this.conf.get("avro.map.output.value.schema"));
    }
}
