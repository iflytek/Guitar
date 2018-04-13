package com.iflytek.guitar.share.avro.mapreduce.input;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;

public class GenericStrDatumReader<D> extends GenericDatumReader<D> {
    @Override
    protected Object readString(Object old, Decoder in) throws IOException {
        return in.readString();
    }
}
