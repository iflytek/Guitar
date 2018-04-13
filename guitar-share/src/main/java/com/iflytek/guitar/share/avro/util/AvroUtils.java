package com.iflytek.guitar.share.avro.util;

import com.iflytek.guitar.share.avro.reflect.ReflectDataEx;
import com.iflytek.guitar.share.utils.Constants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AvroUtils {
  public static final String DATUM_READER_CLASS = "avro.datum.reader.class";

  static DataOutputBuffer out = new DataOutputBuffer();
  static DataInputBuffer in = new DataInputBuffer();
  
  // static ByteArrayOutputStream arrout = new ByteArrayOutputStream();
  
  public static <V> V clone(V v) throws IOException {
    out.reset();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    Schema schema;
    if (GenericContainer.class.isAssignableFrom(v.getClass())) {
        schema = ((GenericContainer) v)
            .getSchema();
    } else {
        schema = ReflectData.get().getSchema(v.getClass());
    }
    
    GenericDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
    GenericDatumReader<V> reader = new ReflectDatumReader<V>(schema);
    writer.write(v, encoder);
    in.reset(out.getData(), out.getLength());
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    return reader.read(null, decoder);
    
  }

  public static <T> T getClone(T value) throws IOException {
    T tmpValue = null;
    if (Constants.IS_TEST) {
      /* windows环境下, map都在一个进程运行, AvroUtils.clone是多线程不可入的, 会报错 */
      synchronized (Constants.IS_TEST) {
        tmpValue = AvroUtils.clone(value);
      }
    } else {
      tmpValue = AvroUtils.clone(value);
    }

    return tmpValue;
  }

  public static void setDatumReader(Configuration conf, Class<? extends DatumReader> classzz) {
    conf.setClass(DATUM_READER_CLASS, classzz, DatumReader.class);

  }

  public static DatumReader getDatumReader(Configuration conf) throws IOException {
    try {
      Class<? extends DatumReader> readerClass = conf.getClass(
              DATUM_READER_CLASS, ReflectDatumReader.class, DatumReader.class);
      try {

        return readerClass.getConstructor(Schema.class, Schema.class,
                ReflectData.class).newInstance(null, null, ReflectDataEx.get());
      } catch (Exception e) {
        return readerClass.newInstance();
      }
    } catch (Exception e) {
      throw new IOException(e);
      // e.printStackTrace();
    }
  }

}
