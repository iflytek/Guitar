package com.iflytek.guitar.share.avro.mapreduce.input;

import com.iflytek.guitar.share.avro.io.Pair;
import com.iflytek.guitar.share.avro.mapreduce.FsInput;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class AvroRecordReader<K, V> extends RecordReader<K, V> {

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

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        reader = new DataFileReader<Object>(new FsInput(fileSplit.getPath(), context.getConfiguration()),
                new GenericStrDatumReader<Object>());
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
        Pair<K, V> tmpPair = null;
        GenericRecord record = (GenericRecord) obj;

        try {
            GenericRecord value = (GenericRecord) record.get("value");
            if (value != null) {
                Object key = (Object) record.get("key");
                if (key != null) {
                    tmpPair = new Pair<K, V>(record.getSchema());
                    tmpPair.set((K) key, (V) value);
                } else {
                    tmpPair = new Pair<K, V>((K) null, Schema.create(Schema.Type.NULL), (V) value, value.getSchema());
                }
            }
        } catch (Exception e) {

        }
        if (tmpPair == null) {
            tmpPair = new Pair<K, V>((K) null, Schema.create(Schema.Type.NULL), (V) obj, record.getSchema());
        }
        return tmpPair;

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
