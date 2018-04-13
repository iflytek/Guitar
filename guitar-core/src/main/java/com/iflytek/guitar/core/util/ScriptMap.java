package com.iflytek.guitar.core.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ScriptMap {
    private Map<String, Object> map = new HashMap<String, Object>();
    private static final Set<Class> SCRIPT_TYPE = new HashSet<Class>() {
        {
            add(Long.class);
            add(Integer.class);
            add(Double.class);
            add(String.class);
        }
    };

    public static ScriptMap getScriptMap() {
        return new ScriptMap();
    }

    public void put(String key, Object value) throws IOException {
        if (null == value) {
            throw new IOException("null == value, where key is " + key);
        }

        if (!SCRIPT_TYPE.contains(value.getClass())) {
            throw new IOException("value's type[" + value.getClass().getName() + "] not support!");
        }

        map.put(key, value);
    }

    public void putString(String key, String value) throws IOException {
        if (null == value) {
            throw new IOException("null == value, where key is " + key);
        }

        map.put(key, value.toString());
    }

    public Object get(String key) {
        return map.get(key);
    }

    public int size() {
        return map.size();
    }

    @Override
    public String toString() {
        return "ScriptMap{" +
                "map=" + map +
                '}';
    }

}
