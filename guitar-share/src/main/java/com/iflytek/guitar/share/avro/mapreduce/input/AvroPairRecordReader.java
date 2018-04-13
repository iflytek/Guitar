package com.iflytek.guitar.share.avro.mapreduce.input;

import com.iflytek.guitar.share.avro.io.Pair;
import com.iflytek.guitar.share.avro.mapreduce.FsInput;
import com.iflytek.guitar.share.avro.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class AvroPairRecordReader<K, V> extends RecordReader<K, V> {

    private DataFileReader<Object> reader;
    private long start;
    private long end;
    private Pair<K, V> pair = null;
    private Object reuse = null;

    @Override
    public float getProgress() throws IOException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getPos() - start) / (float) (end - start));
        }
    }

    public long getPos() throws IOException {
        return reader.previousSync();
    }

    @Override
    public void close() throws IOException {
        if (null != reader) {
            reader.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        DatumReader datumReader = AvroUtils.getDatumReader(
                context.getConfiguration());
        reader = new DataFileReader<Object>(new FsInput(fileSplit.getPath(),
                context.getConfiguration()), datumReader);

        String expectedSchema = context.getConfiguration().get(
                "avro.schema.expected", null);
        if (expectedSchema != null && datumReader instanceof ReflectDatumReader) {
            ((ReflectDatumReader) datumReader).setExpected(Schema.parse(expectedSchema, false));
        }

        reader.sync(fileSplit.getStart()); // sync to start
        this.start = reader.previousSync();
        this.end = fileSplit.getStart() + split.getLength();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!reader.hasNext() || reader.pastSync(end)) {
            return false;
        }
        pair = Convert2Pair(reader.next(reuse));
        return true;
    }

    @SuppressWarnings("unchecked")
    public Pair<K, V> Convert2Pair(Object obj) {
        if (obj instanceof Pair) {
            return (Pair<K, V>) obj;
        } else if (GenericRecord.class.isAssignableFrom(obj.getClass())) {
            GenericRecord record = (GenericRecord) obj;
            if (Pair.isPairSchema(record.getSchema())) {
                pair = new Pair<K, V>(record.getSchema());
                pair.set((K) record.get("key"), (V) record.get("value"));
            } else {
                pair = new Pair<K, V>((K) null, Schema.create(Schema.Type.NULL),
                        (V) obj, record.getSchema());
            }
        } else {
            pair = new Pair<K, V>((K) null, Schema.create(Schema.Type.NULL), (V) obj,
                    ReflectData.get().getSchema(obj.getClass()));
        }
        return pair;

    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return pair.key();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return pair.value();
    }

}
