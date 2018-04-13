package com.iflytek.guitar.share.avro.mapreduce.output;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * A extends class for {@link FileOutputFormat}s that read from
 * {@link FileSystem}s.
 */
public abstract class ExtFileOutputFormat<K, V> extends FileOutputFormat<K, V> {
    protected static final String BASE_OUTPUT_NAME = "mapreduce.output.basename";
    protected static final String PART = "part";

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
            throws IOException {
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getUniqueFile(context,
                getOutputName(context), extension));
    }

    /**
     * Get the base output name for the output file.
     */
    protected static String getOutputName(JobContext job) {
        return job.getConfiguration().get(BASE_OUTPUT_NAME, PART);
    }

    /**
     * Set the base output name for output file to be created.
     */
    public static void setOutputName(JobContext job, String name) {
        job.getConfiguration().set(BASE_OUTPUT_NAME, name);
    }
}
