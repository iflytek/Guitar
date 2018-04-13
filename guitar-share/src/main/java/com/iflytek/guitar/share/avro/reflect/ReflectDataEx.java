package com.iflytek.guitar.share.avro.reflect;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class ReflectDataEx extends ReflectData {
	@SuppressWarnings("rawtypes")
	@Override
	public Object newRecord(Object old, Schema schema) {
		Class c = getClass(schema);
		if (c == null) {
            return super.newRecord(old, schema); // punt to generic
        }
		if (old == null) {
            return newInstance(c, schema);
        }
		return (c == old.getClass() ? old : newInstance(c, schema));
	}

	private static final ReflectDataEx INSTANCE = new ReflectDataEx();

	public static ReflectDataEx get() {
		return INSTANCE;
	}

	@Override
	public Schema getSchema(java.lang.reflect.Type type) {
		if (type == null) {
            return null;
        } else {
            return super.getSchema(type);
        }
	}

}
