package com.iflytek.guitar.share.avro.serializer;

import com.iflytek.guitar.share.avro.mapreduce.ConfigSchemaData;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class AvroDXSerialization<T> extends Configured implements Serialization<T> {
  
  public static final String AVRO_SCHEMA_KEY = "Avro-Schema";
  public static DecoderFactory decoderFactory = new DecoderFactory();
  public static EncoderFactory encoderFactory = new EncoderFactory();
  
  @Override
  public Deserializer<T> getDeserializer(Class<T> c) {
    return new AvroDeserializer(c);
  }
  
  @Override
  public Serializer<T> getSerializer(Class<T> c) {
    return new AvroSerializer(c);
  }
  
  /**
   * Return an Avro Schema instance for the given class.
   */
  public abstract Schema getSchema(T t);
  
  /**
   * Create and return Avro DatumWriter for the given class.
   */
  public abstract DatumWriter<T> getWriter(Class<T> clazz);
  
  /**
   * Create and return Avro DatumReader for the given class.
   */
  public abstract DatumReader<T> getReader(Class<T> clazz);
  
  class AvroSerializer implements Serializer<T> {
    
    private DatumWriter<T> writer;
    private BinaryEncoder encoder;
    private OutputStream outStream;
    
    AvroSerializer(Class<T> clazz) {
      this.writer = getWriter(clazz);
    }
    
    @Override
    public void close() throws IOException {
      encoder.flush();
      outStream.close();
    }
    
    @Override
    public void open(OutputStream out) throws IOException {
      outStream = out;
      encoder = encoderFactory.directBinaryEncoder(out, null);
    }
    
    @Override
    public void serialize(T t) throws IOException {
      // writer.setSchema(getSchema(t));
      writer.write(t, encoder);
    }
    
  }
  
  class AvroDeserializer implements Deserializer<T> {
    
    private DatumReader<T> reader;
    private BinaryDecoder decoder;
    private InputStream inStream;
    
    AvroDeserializer(Class<T> clazz) {
      this.reader = getReader(clazz);
    }
    
    @Override
    public void close() throws IOException {
      inStream.close();
    }
    
    @Override
    public T deserialize(T t) throws IOException {
      T tt = reader.read(t, decoder);
      if (tt != null && ConfigSchemaData.class.isAssignableFrom(tt.getClass())) {
        ((ConfigSchemaData) tt).setConf(getConf());
      }
      return tt;
    }
    
    @Override
    public void open(InputStream in) throws IOException {
      inStream = in;
      decoder = decoderFactory.directBinaryDecoder(in, null);
    }
    
  }
  
}
