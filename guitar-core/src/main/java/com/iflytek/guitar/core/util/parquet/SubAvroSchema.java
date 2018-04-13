package com.iflytek.guitar.core.util.parquet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.iflytek.guitar.share.avro.reflect.ReflectDataEx;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.util.*;

public class SubAvroSchema {
    private static final String PATH_TOKEN = ".";

    public static void setFieldMap(Schema srcSchema, Map<String, Schema.Field> fieldMap, String[] fieldNames, int index) {
        if (index >= fieldNames.length) {
            return;
        }
        if (srcSchema.getType() == Schema.Type.RECORD) {
            for (Schema.Field field : srcSchema.getFields()) {
                if (fieldNames[index].equals(field.name())) {
                    fieldMap.put(getPathString(fieldNames, index),
                            new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
                    setFieldMap(field.schema(), fieldMap, fieldNames, index + 1);
                    break;
                }
            }
        }
    }

    public static String getPathString(String[] fieldNames, int index) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(fieldNames[0]);
        int i = 1;
        while (i <= index) {
            stringBuilder.append(PATH_TOKEN).append(fieldNames[i]);
            i++;
        }
        return stringBuilder.toString();
    }

    public static Collection<Schema.Field> generateFields(Map<String, Schema.Field> path2Field,
                                                          Map<Integer, Set<String>> layer2Paths,
                                                          Map<Integer, Set<String>> layer2TruePaths,
                                                          int layer,
                                                          Map<String, Schema.Field> bottomFields) {

        if (layer == 1) {
            return bottomFields.values();
        }

        Set<String> bottomLayerPaths = Sets.newHashSet();
        bottomLayerPaths.addAll(layer2Paths.get(layer));

        Map<String, List<String>> fatherPathMap = Maps.newHashMap();
        for (String bottomPath : bottomLayerPaths) {
            String fatherPath = bottomPath.substring(0, bottomPath.lastIndexOf(PATH_TOKEN));
            if (fatherPathMap.containsKey(fatherPath)) {
                fatherPathMap.get(fatherPath).add(bottomPath);
            } else {
                List<String> paths = Lists.newArrayList();
                paths.add(bottomPath);
                fatherPathMap.put(fatherPath, paths);
            }
        }

        Map<String, Schema.Field> subBottomFields = Maps.newHashMap();

        if (layer2TruePaths.containsKey(layer - 1)) {
            Set<String> subBottomLayerPaths = Sets.newHashSet();
            subBottomLayerPaths.addAll(layer2TruePaths.get(layer - 1));
            Set<String> mergePathSet = new HashSet<String>();
            mergePathSet.addAll(subBottomLayerPaths);
            mergePathSet.retainAll(fatherPathMap.keySet());
            if (mergePathSet.size() > 0) {
                for (String mergePath : mergePathSet) {
                    subBottomFields.put(mergePath, path2Field.get(mergePath));
                    fatherPathMap.remove(mergePath);
                }
            }
        }

        for (String fatherPath : fatherPathMap.keySet()) {
            Schema.Field fatherField = path2Field.get(fatherPath);
            Schema resSchema = Schema.createRecord(fatherField.schema().getName(), null, null, false);
            List<Schema.Field> fields = new ArrayList<Schema.Field>();
            for (String sonPath : fatherPathMap.get(fatherPath)) {
                Schema.Field sonField = null;
                if (bottomFields.containsKey(sonPath)) {
                    sonField = bottomFields.get(sonPath);
                } else {
                    sonField = path2Field.get(sonPath);
                }
                fields.add(new Schema.Field(sonField.name(), sonField.schema(), sonField.doc(), sonField.defaultValue(), sonField.order()));
            }
            resSchema.setFields(fields);

            subBottomFields.put(fatherPath,
                    new Schema.Field(fatherField.name(), resSchema, fatherField.doc(), fatherField.defaultValue(), fatherField.order()));
        }

        bottomFields = subBottomFields;

        return generateFields(path2Field, layer2Paths, layer2TruePaths, layer - 1, bottomFields);
    }

    public static MessageType getMTByFields(Class srcClassz, Set<String> fieldNames, Configuration conf) {
        Schema srcSchema = ReflectDataEx.get().getSchema(srcClassz);

        Schema resSchema = Schema.createRecord(srcSchema.getName(), null, srcSchema.getNamespace(), false);
        List<Schema.Field> fields = new ArrayList<Schema.Field>();

        Map<String, Schema.Field> fieldMap = Maps.newHashMap();
        Set<String> nestPaths = Sets.newHashSet();
        for (String fieldname : fieldNames) {
            if (!fieldname.contains(PATH_TOKEN)) {
                for (Schema.Field field : srcSchema.getFields()) {
                    if (fieldname.equals(field.name())) {
                        fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
                        break;
                    }
                }
            } else {
                nestPaths.add(fieldname);
                String[] splitfields = fieldname.split("\\.");
                setFieldMap(srcSchema, fieldMap, splitfields, 0);
            }
        }

        if (fieldMap.size() > 0) {
            int lastLayer = 0;
            Map<Integer, Set<String>> layer2Paths = Maps.newHashMap();
            Map<Integer, Set<String>> layer2TruePaths = Maps.newHashMap();
            for (String path : fieldMap.keySet()) {
                int currentLayer = path.split("\\.").length;
                if (currentLayer > lastLayer) {
                    lastLayer = currentLayer;
                }
                if (layer2Paths.containsKey(currentLayer)) {
                    layer2Paths.get(currentLayer).add(path);
                } else {
                    Set<String> paths = Sets.newHashSet();
                    paths.add(path);
                    layer2Paths.put(currentLayer, paths);
                }
            }

            for (String path : nestPaths) {
                int currentLayer = path.split("\\.").length;
                if (layer2TruePaths.containsKey(currentLayer)) {
                    layer2TruePaths.get(currentLayer).add(path);
                } else {
                    Set<String> paths = Sets.newHashSet();
                    paths.add(path);
                    layer2TruePaths.put(currentLayer, paths);
                }
            }

            Map<String, Schema.Field> bottomFields = Maps.newHashMap();
            for (String path : layer2Paths.get(lastLayer)) {
                bottomFields.put(path, fieldMap.get(path));
            }
            Collection<Schema.Field> res = generateFields(fieldMap, layer2Paths, layer2TruePaths, lastLayer, bottomFields);

            List<Schema.Field> filterFields = Lists.newArrayList();
            for (Schema.Field field1 : res) {
                boolean flag = true;
                for (Schema.Field field2 : fields) {
                    if (field1.name().equals(field2.name())) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    filterFields.add(field1);
                }
            }
            fields.addAll(filterFields);
        }

        resSchema.setFields(fields);

        conf.set("parquet.avro.write-old-list-structure", "false");

        return new AvroSchemaConverter(conf).convert(resSchema);
    }

}
