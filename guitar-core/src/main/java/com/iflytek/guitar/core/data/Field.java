package com.iflytek.guitar.core.data;

import com.iflytek.guitar.core.util.ConstantsAlg;

import java.io.IOException;
import java.util.List;

public abstract class Field {

    // 该字段为排序后报表维度序列中的下标
    protected int sortedIndex = -1;

    private Field() {
    }

    protected Field(String field, List<String> sortedFieds) throws IOException {
        setSortedIndex(field, sortedFieds);
    }

    protected Field(int idx) {
        setSortedIndex(idx);
    }

    public void setSortedIndex(int idx) {
        sortedIndex = idx;
    }

    public void setSortedIndex(String filedName, List<String> sortedFieds) throws IOException {
        boolean find = false;
        if (filedName != null && filedName.length() > 0 && sortedFieds != null) {
            for (int i = 0; i < sortedFieds.size(); i++) {
                if (filedName.equalsIgnoreCase(sortedFieds.get(i))) {
                    sortedIndex = i;
                    find = true;
                    break;
                }
            }
        }
        if (!find) {
            throw new IOException("Cannot find field " + filedName);
        }
    }

    public abstract boolean setValue(ReportKey rk, String v) throws IOException;

    public boolean setALL(ReportKey rk) throws IOException {
        return setValue(rk, ConstantsAlg.CHAR_ALL);
    }
}
