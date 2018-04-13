package com.iflytek.guitar.share.avro.mapreduce;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

/**
 * Adapt an {@link FSDataInputStream} to {@link SeekableInput}.
 */
public class FsInput implements Closeable, SeekableInput {
    private final FSDataInputStream stream;
    private final long len;

    /**
     * Construct given a path and a configuration.
     */
    public FsInput(Path path, Configuration conf) throws IOException {
        this.stream = path.getFileSystem(conf).open(path);
        this.len = path.getFileSystem(conf).getFileStatus(path).getLen();
    }

    @Override
    public long length() {
        return len;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }

    @Override
    public void seek(long p) throws IOException {
        stream.seek(p);
    }

    @Override
    public long tell() throws IOException {
        return stream.getPos();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
