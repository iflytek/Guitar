package com.iflytek.guitar.core.alg;

import com.iflytek.guitar.core.util.ConstantsAlg;

import java.util.*;

public class Input {
    public List<String> paths = new ArrayList<String>();
    public DataType data_type = null;
    public FileType file_type = null;
    public CheckType check_type = null;

    public String parquet_schema_class = null;
    public Set<String> parquet_schema_subfields = null;

    public void check() throws Exception {
        if (paths == null || paths.isEmpty()) {
            throw new Exception("Configure Input Error: input Path is Empty");
        }
        if (null == data_type) {
            throw new Exception("Configure Input Error: data_type is Empty");
        }
        if (null == file_type) {
            throw new Exception("Configure Input Error: file_type is Empty");
        }
        if (null == check_type) {
            throw new Exception("Configure Input Error: check_type is Empty");
        }
    }

    public enum DataType {
        json,         // json
        textline,    // text
        struct       // struct or class
    }

    public enum FileType {
        avro,         // apache avro
        seqfile,     // sequence file
        text,        // text file
        parquet,    // parquet file
        orc         // orc file
    }

    public enum CheckType {
        no,          // no check
        part,       // must have input
        all;        // all input path must be exist

        public boolean bigger(CheckType o) {
            return this.compareTo(o) > 0;
        }

    }

    private String dateReplace(String dirFormat, Calendar cal) {
        String strYear = String.format("%04d", cal.get(Calendar.YEAR));
        String strMonth = String.format("%02d", cal.get(Calendar.MONTH) + 1);
        String strDay = String.format("%02d", cal.get(Calendar.DAY_OF_MONTH));
        String strHour = String.format("%02d", cal.get(Calendar.HOUR_OF_DAY));

        String dateDir = dirFormat.replace(ConstantsAlg.YEAR_DATE_WORDCARD, strYear);
        dateDir = dateDir.replace(ConstantsAlg.MONTH_DATE_WORDCARD, strMonth);
        dateDir = dateDir.replace(ConstantsAlg.DAY_DATE_WORDCARD, strDay);
        dateDir = dateDir.replace(ConstantsAlg.HOUR_DATE_WORDCARD, strHour);

        return dateDir;
    }

    // 输入数据做日期替换
    // 没有%H（写死小时或没有小时的），加载一次
    public List<String> getPathOrigin(AlgDef.Frequence freq, Date startDate) {
        List<String> ret = new ArrayList<String>();

        Date endDate = freq.getEndDate(startDate);
        Calendar cal_start = Calendar.getInstance();
        Calendar cal_end = Calendar.getInstance();

        for (String path_conf : paths) {
            cal_start.setTime(startDate);
            cal_end.setTime(endDate);
            if (!path_conf.contains(ConstantsAlg.HOUR_DATE_WORDCARD)) {
                String path = dateReplace(path_conf, cal_start);
                ret.add(path);
            } else {
                while (cal_start.before(cal_end)) {
                    String path = dateReplace(path_conf, cal_start);
                    ret.add(path);
                    cal_start.add(Calendar.HOUR, 1);
                }
            }
        }

        return ret;
    }

    @Override
    public String toString() {
        return "Input{" +
                "\npaths=" + paths +
                "\n, data_type=" + data_type +
                "\n, file_type=" + file_type +
                "\n, check_type=" + check_type +
                "\n, parquet_schema_class=" + parquet_schema_class +
                '}';
    }

}
