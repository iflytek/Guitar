package com.iflytek.guitar.core.util;

import com.iflytek.guitar.core.data.dataformat.DataRecord;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class ScriptOper {
    public static Object CUSTOBJECT = null;

    private String filePath = null;
    private String fileName = null;
    private Date fileDate = null;

    protected Map<String, Object> confValues = null;

    public void parse(DataRecord record, List<ScriptMap> lstMap) {
    }

    public static void setCustomObject(Object scriptObject) {
        CUSTOBJECT = scriptObject;
    }

    public void setFilePath(String path) {
        filePath = path;
    }

    public void setFileName(String name) {
        fileName = name;
    }

    public void setReportDate(Date date) {
        fileDate = date;
    }

    public void setConfValues(Map<String, Object> v) {
        confValues = v;
    }

    public final String getFilePath() {
        return filePath;
    }

    public final String getFileName() {
        return fileName;
    }

    public final Date getReportDate() {
        return fileDate;
    }

    public final Map<String, Object> getConfValues() {
        return confValues;
    }

}
