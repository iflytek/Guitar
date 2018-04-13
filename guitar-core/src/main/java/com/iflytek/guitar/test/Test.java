package com.iflytek.guitar.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.*;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws Exception {
//        parquetWriter("testParquet");
        parquetReaderV2("testParquet");

    }


    /**
     * @param outPath 　　输出Parquet格式
     * @throws IOException
     */
    static void parquetWriter(String outPath) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                " required binary city (UTF8);\n" +
                " required binary ip (UTF8);\n" +
                " repeated group time {\n" +
                " required int32 ttl;\n" +
                " required int32 ttl2;\n" +
                "}\n" +
                "}");

        MessageType schema2 = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("city")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ip")
                .repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("ttl")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("ttl2")
                .named("time")
                .named("Pair");

        GroupFactory factory = new SimpleGroupFactory(schema);

        Configuration configuration = new Configuration();

        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, configuration);

        Path path = new Path(outPath);

        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);

        Group group1 = factory.newGroup()
                .append("city", "hefei")
                .append("ip", "192.168.1.1");
        Group tmpG1 = group1.addGroup("time");
        tmpG1.append("ttl", 1000);
        tmpG1.append("ttl2", 2000);

        writer.write(group1);

        Group group2 = factory.newGroup()
                .append("city", "nanjing")
                .append("ip", "192.168.2.2");
        Group tmpG2 = group2.addGroup("time");
        tmpG2.append("ttl", 3000);
        tmpG2.append("ttl2", 4000);

        writer.write(group2);

        writer.close();
    }

    static void parquetReaderV2(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();

        ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, new Path(inPath));

        Configuration configuration = new Configuration();
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, "message Pair {\n" +
                " required binary city (UTF8);\n" +
                " required binary ip (UTF8);\n" +
                " optional binary ip2 (UTF8);\n" +
                "}");

        ParquetReader<Group> reader = builder.withConf(configuration).build();

        Group line = reader.read();
        Group time = line.getGroup("time", 0);

        System.out.println(line.toString());

        //通过下标和字段名称都可以获取
        System.out.println(
                line.getString(0, 0) + "\t" +
                line.getString(1, 0) + "\t"
//                        +
//                    time.getInteger(0, 0) + "\t" +
//                    time.getInteger(1, 0) + "\t"
        );

        System.out.println(
                line.getString("city", 0) + "\t" +
                line.getString("ip", 0) + "\t"
//                        +
//                    time.getInteger("ttl", 0) + "\t" +
//                    time.getInteger("ttl2", 0) + "\t"
        );


    }

    //新版本中new ParquetReader()所有构造方法好像都弃用了,用上面的builder去构造对象
    static void parquetReader(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<Group>(new Path(inPath), readSupport);
        Group line = null;
        while ((line = reader.read()) != null) {
            System.out.println(line.toString());
        }
        System.out.println("读取结束");

    }

}



