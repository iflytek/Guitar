package com.iflytek.guitar.share.avro.mapreduce.input;

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

public class AvroRecordInputFormat<K, V> extends FileInputFormat<K, V> {

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new AvroRecordReader<K, V>();
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
//                files.set(i, fs.getFileStatus(new Path(p, MapAvroFile.DATA_FILE_NAME)));
                files.set(i, fs.getFileStatus(new Path(p, "data.avro")));
            }
        }
        return files;
    }

    @SuppressWarnings("rawtypes")
//    public static void setDatumReader(Configuration conf, Class<? extends DatumReader> classzz) {
//        MapAvroFile.Reader.setDatumReader(conf, classzz);
//    }

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
