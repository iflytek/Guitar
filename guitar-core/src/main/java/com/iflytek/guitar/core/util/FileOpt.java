package com.iflytek.guitar.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FileOpt {

    public static long getPathSize(Path path, Configuration conf) throws IOException {
        long size = 0;
        FileSystem fs = path.getFileSystem(conf);
        if (fs.isFile(path)) {
            size = fs.getFileStatus(path).getLen();
        } else {
            FileStatus[] fstatus = fs.listStatus(path);
            for (FileStatus fst : fstatus) {
                if (fst.isDirectory()) {
                    size += getPathSize(fst.getPath(), conf);
                } else {
                    size += fst.getLen();
                }
            }
        }
        return size;
    }

    public static List<Path> parseWordcardDir(FileSystem fs, String wordcardDir) throws IOException {
        List<Path> lstPath = Lists.newArrayList();

        FileStatus[] aStatus = fs.globStatus(new Path(wordcardDir));
        if (null == aStatus || aStatus.length <= 0) {
            return lstPath;
        }

        Path[] tmpLstPath = FileUtil.stat2Paths(aStatus);
        lstPath.addAll(Arrays.asList(tmpLstPath));

        return lstPath;
    }

    public static Integer getCombReduceNum(Iterable<String> inputs, long reduceSplitSize, int numAlgs, Configuration conf) throws IOException {
        if (null != inputs && reduceSplitSize > 0) {
            long fileSize = 0;
            FileSystem fs = FileSystem.get(conf);
            for (String input : inputs) {
                List<Path> paths = parseWordcardDir(fs, input);
                for (Path path : paths) {
                    if (fs.exists(path)) {
                        fileSize += getPathSize(path, conf);
                    }
                }
            }

            return ((int) ((fileSize / reduceSplitSize) * Math.sqrt((double) numAlgs))) + 1;
        }

        return null;
    }

}
