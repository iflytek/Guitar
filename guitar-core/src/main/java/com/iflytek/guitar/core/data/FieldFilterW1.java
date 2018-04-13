package com.iflytek.guitar.core.data;

import java.io.IOException;
import java.util.List;

public class FieldFilterW1 extends FieldFilter {
    public FieldFilterW1(String field, List<String> sortedFieds) throws IOException {
        super(field, sortedFieds);
    }

    @Override
    public boolean setValue(ReportKey rk, String v) throws IOException {
        if (list.contains(rk.get(sortedIndex))) {
            rk.set(sortedIndex, v);
            return true;
        }
        return false;
    }
}
