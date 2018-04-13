package com.iflytek.guitar.share.avro.mapreduce.input;

import com.iflytek.guitar.share.avro.util.AvroUtils;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

public class AvroPairInputFormat<K, V> extends FileInputFormat<K, V> {

    /**
     * The name of the data file.
     */
    public static final String DATA_FILE_NAME = "data.avro";

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
                                                 TaskAttemptContext context) throws IOException, InterruptedException {
        return new AvroPairRecordReader<K, V>();
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {

        List<FileStatus> files = super.listStatus(job);
        int len = files.size();
        for (int i = 0; i < len; ++i) {
            FileStatus file = files.get(i);
            if (file.isDirectory()) { // it's a MapFile
                Path p = file.getPath();
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                // use the data file
                files.set(i, fs.getFileStatus(new Path(p, DATA_FILE_NAME)));
            }
        }
        return files;
    }

    @SuppressWarnings("rawtypes")
    public static void setDatumReader(Configuration conf,
                                      Class<? extends DatumReader> classzz) {
        AvroUtils.setDatumReader(conf, classzz);

    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // try {
        // List<FileStatus> files = super.listStatus(context);
        // for(FileStatus fs : files)
        // {
        // //System.out.println(fs.getPath().toString().toLowerCase());
        // if(fs.getPath().toString().toLowerCase().contains("file:/"))
        // return false;
        // }
        // } catch (IOException e) {
        // e.printStackTrace();
        // return true;
        // }
        if (filename.toString().startsWith("file:/")) {
            return false;
        }
        return true;
    }
}
