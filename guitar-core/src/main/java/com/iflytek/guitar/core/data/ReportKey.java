package com.iflytek.guitar.core.data;

import com.clearspring.analytics.hash.MurmurHash;
import com.iflytek.guitar.share.avro.util.AvroUtils;
import com.iflytek.guitar.share.db.Exportable;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportKey implements Comparable<ReportKey>, Exportable {
    private static Schema schema = ReflectData.get().getSchema(ReportKey.class);
    public static Map<String, List<String>> algFeildsDef = new HashMap<String, List<String>>();
    public String reportName = "";
    //    public Map<String, String> reportFeild = new HashMap<String, String>();
    public String[] reportFeild;

    public ReportKey() {
    }

    public ReportKey(String name, int len) {
        setReportName(name);
        reportFeild = new String[len];
    }

    public void setReportName(String name) {
        if (null == name) {
            reportName = "";
        } else {
            reportName = name;
        }
    }

    public String getReportName() {
        return reportName;
    }

    @Override
    public String toString() {
        String ret = reportName + "|";
        for (String f : reportFeild) {
            ret += f + ",";
        }
        return ret;
    }


    public void set(int idx, String value) throws IOException {
        reportFeild[idx] = value;
    }

    public String get(int idx) {
        return reportFeild[idx];
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof ReportKey)) {
            return false;
        }

        ReportKey that = (ReportKey) o;
        if (!reportName.equals(that.reportName)) {
            return false;
        }
        // return (0 == ReflectData.get().compare(this, that, schema));

        if (reportFeild.length != that.reportFeild.length) {
            return false;
        }

        for (int i = 0; i < reportFeild.length; i++) {
            if (!reportFeild[i].equals(that.reportFeild[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return MurmurHash.hash(this.toString());
    }

    @Override
    public int compareTo(ReportKey that) {
        //return ReflectData.get().compare(this, that, schema);
        /*
         * 用avro自反射的方式，和自实现的方式性能比较：
         * 重复调用100,0000次，性能分别为：
         * avro自反射： 1000ms  左右
         * 自实现：     70ms    左右
         */

        int ret = reportName.compareTo(that.reportName);
        if (ret != 0) {
            return ret;
        }
        int len = Math.min(reportFeild.length, that.reportFeild.length);
        for (int i = 0; i < len; i++) {
            ret = reportFeild[i].compareTo(that.reportFeild[i]);
            if (0 != ret) {
                return ret;
            }
        }

        return (reportFeild.length - that.reportFeild.length);
    }

    public ReportKey getClone() throws IOException {
        return AvroUtils.getClone(this);
    }

    @Override
    public Map<String, String> getExportFeilds() {
        Map<String, String> ret = new HashMap<String, String>();
        List<String> sortedDims = algFeildsDef.get(reportName);
        for (int i = 0; i < sortedDims.size(); i++) {
            String dim = sortedDims.get(i);
            ret.put(dim, reportFeild[i]);
        }
        return ret;
    }

    @Override
    public ReportKey clone() {
        ReportKey rk = new ReportKey(this.reportName, this.reportFeild.length);
        for (int i = 0; i < this.reportFeild.length; i++) {
            rk.reportFeild[i] = reportFeild[i];
        }
        return rk;
    }

}
