package com.iflytek.guitar.core.data;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FieldFilterW2 extends FieldFilter {
    private int fieldSortedIndexRelate = -1;

    public FieldFilterW2(String field, List<String> sortedFieds, String relateField) throws IOException {
        super(field, sortedFieds);
        setFieldRelate(relateField, sortedFieds);
    }

    public void setFieldRelate(String filedName, List<String> sortedFieds) throws IOException {
        boolean find = false;
        if (null != filedName && filedName.length() > 0 && sortedFieds != null) {
            for (int i = 0; i < sortedFieds.size(); i++) {
                if (filedName.equalsIgnoreCase(sortedFieds.get(i))) {
                    fieldSortedIndexRelate = i;
                    find = true;
                    break;
                }
            }
        }
        if (!find) {
            throw new IOException("Cannot find filed " + filedName);
        }
    }

    @Override
    public void setList(Map<String, String> values) {
        if (null != values) {
            for (Map.Entry<String, String> entry : values.entrySet()) {
                list.add(entry.getKey() + "\t" + entry.getValue());
            }
        }
    }

    @Override
    public boolean setValue(ReportKey rk, String v) throws IOException {
        String comp = rk.get(fieldSortedIndexRelate) + "\t" + rk.get(sortedIndex);
        if (list.contains(comp)) {
            rk.set(sortedIndex, v);
            return true;
        }
        return false;
    }

    public int getRelateIdx() {
        return this.fieldSortedIndexRelate;
    }

}
