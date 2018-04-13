package com.iflytek.guitar.core.data;

import java.io.IOException;
import java.util.List;

public class FieldIgnore extends Field {
    public FieldIgnore(String field, List<String> sortedFieds) throws IOException {
        super(field, sortedFieds);
    }

    public FieldIgnore(int idx) {
        super(idx);
    }

    @Override
    public boolean setValue(ReportKey rk, String v) throws IOException {
        throw new IOException("Ignore Field should not called by setValue!");
    }
}
