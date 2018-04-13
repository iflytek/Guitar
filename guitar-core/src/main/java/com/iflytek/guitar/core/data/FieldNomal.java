package com.iflytek.guitar.core.data;

import java.io.IOException;
import java.util.List;

public class FieldNomal extends Field {
    public FieldNomal(String field, List<String> sortedFieds) throws IOException {
        super(field, sortedFieds);
    }

    @Override
    public boolean setValue(ReportKey rk, String v) throws IOException {
        rk.set(this.sortedIndex, v);
        return true;
    }
}
