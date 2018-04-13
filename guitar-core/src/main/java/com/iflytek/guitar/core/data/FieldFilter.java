package com.iflytek.guitar.core.data;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class FieldFilter extends Field {
    protected Set<String> list = new HashSet<String>();

    public FieldFilter(String field, List<String> sortedFieds) throws IOException {
        super(field, sortedFieds);
    }

    public void setList(String v, String split) {
        if (null != v && v.length() > 0 && split != null && split.length() > 0) {
            String[] vs = v.split(split);
            for (String sv : vs) {
                list.add(sv);
            }
        }
    }

    public void setList(List<String> values) {
        if (null != values) {
            for (String v : values) {
                list.add(v);
            }
        }
    }

    public void setList(Map<String, String> values) {
        if (null != values) {
            for (String v : values.keySet()) {
                list.add(v);
            }
        }
    }

    public void setList(Set<String> values) {
        for (String v : values) {
            list.add(v);
        }
    }
}
