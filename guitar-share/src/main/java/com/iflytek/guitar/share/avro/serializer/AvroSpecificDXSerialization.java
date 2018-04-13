package com.iflytek.guitar.share.avro.serializer;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

@SuppressWarnings("unchecked")
public class AvroSpecificDXSerialization extends AvroDXSerialization<SpecificRecord> {
  
  @Override
  public boolean accept(Class<?> c) {
    return SpecificRecord.class.isAssignableFrom(c);
  }
  
  @Override
  public DatumReader getReader(Class<SpecificRecord> clazz) {
    try {
      return new SpecificDatumReader(clazz.newInstance().getSchema());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public Schema getSchema(SpecificRecord t) {
    return t.getSchema();
  }
  
  @Override
  public DatumWriter getWriter(Class<SpecificRecord> clazz) {
    return new SpecificDatumWriter();
  }
  
}
