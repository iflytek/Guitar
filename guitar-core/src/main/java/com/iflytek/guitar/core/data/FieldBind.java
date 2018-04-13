package com.iflytek.guitar.core.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FieldBind extends Field {
    //绑定字段，不包含本字段自身
    public List<Integer> bindFieds = new ArrayList<Integer>();

    public FieldBind(String field, List<String> sortedFieds, List<String> bindField) throws IOException {
        super(field, sortedFieds);
        if (bindField != null) {
            for (String bfield : bindField) {
                if (!field.equalsIgnoreCase(bfield)) {
                    addBindField(bfield, sortedFieds);
                }
            }
        }
    }

    public void addBindField(String field, List<String> sortedFieds) throws IOException {
        boolean find = false;
        if (null != field && field.length() > 0) {
            for (int i = 0; i < sortedFieds.size(); i++) {
                if (field.equalsIgnoreCase(sortedFieds.get(i))) {
                    bindFieds.add(i);
                    find = true;
                    break;
                }
            }
        }

        if (!find) {
            throw new IOException("Cannot find field " + field);
        }
    }

    @Override
    public boolean setValue(ReportKey rk, String v) throws IOException {
        rk.set(sortedIndex, v);
        for (int idx : bindFieds) {
            rk.set(idx, v);
        }
        return true;
    }
}
