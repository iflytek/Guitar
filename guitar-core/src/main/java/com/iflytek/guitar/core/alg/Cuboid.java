package com.iflytek.guitar.core.alg;

import com.iflytek.guitar.core.data.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class Cuboid {
    private static final Log LOG = LogFactory.getLog(Cuboid.class);

    public static final String CUBOID_FILTER_TYPE_W = "WList"; //白名单
    public static final String CUBOID_FILTER_TYPE_B = "BList"; //黑名单
    public static final String CUBOID_FILTER_SOURCE_C = "config"; //名单来源配置
    public static final String CUBOID_FILTER_SOURCE_L = "list"; //名单来源列表

    public List<CuboidRule> cuboidRules = new ArrayList<CuboidRule>();

    public Field[] getFields(List<String> sortedDims, Map<String, Object> confs) throws IOException {
        Field[] fields = new Field[sortedDims.size()];

        Map<String, Integer> dimsMap = new HashMap<String, Integer>(sortedDims.size());
        for (int i = 0; i < sortedDims.size(); i++) {
            dimsMap.put(sortedDims.get(i).toLowerCase(), i);
        }

        // init
        for (int i = 0; i < sortedDims.size(); i++) {
            fields[i] = new FieldNomal(sortedDims.get(i), sortedDims);
        }

        for (CuboidRule cr : cuboidRules) {
            if (cr instanceof CuboidRuleIgnore) {
                for (String f : ((CuboidRuleIgnore) cr).field_ignore) {
                    Integer fidx = dimsMap.get(f.toLowerCase());
                    if (null == fidx) {
                        throw new IOException("Cannot find field definition named " + f);
                    }
                    setFieldIgnore(fidx, fields);
                }
            } else if (cr instanceof CuboidRuleBind) {
                boolean isFirst = true;
                for (String f : ((CuboidRuleBind) cr).field_bind) {
                    Integer fidx = dimsMap.get(f.toLowerCase());
                    if (null == fidx) {
                        throw new IOException("Cannot find field definition named " + f);
                    }
                    if (isFirst) {
                        if (!setFieldBind(fidx, f, sortedDims, ((CuboidRuleBind) cr).field_bind, fields)) {
                            break;
                        }
                        isFirst = false;
                    } else {
                        setFieldIgnore(fidx, fields);
                    }
                }
            } else if (cr instanceof CuboidRuleFilter) {
                CuboidRuleFilter crf = ((CuboidRuleFilter) cr);
                Integer fidx = dimsMap.get(crf.dimension);
                if (null == fidx) {
                    throw new IOException("Cannot find field definition named " + crf.dimension);
                }
                if (fields[fidx] instanceof FieldBind) {
                    LOG.warn("Field " + crf.dimension + " is bind, will not be set as filter");
                } else if (fields[fidx] instanceof FieldIgnore) {
                    LOG.warn("Field " + crf.dimension + " is ignore, will not be set as filter");
                } else if (fields[fidx] instanceof FieldFilter) {
                    LOG.warn("Field " + crf.dimension + " is already filter, will not be set as filter again");
                } else {
                    if (CUBOID_FILTER_TYPE_B.equalsIgnoreCase(crf.type)) {
                        if (null != crf.dimension_relate && dimsMap.get(crf.dimension_relate.toLowerCase()) != null) {
                            fields[fidx] = new FieldFilterB2(crf.dimension, sortedDims, crf.dimension_relate);
                            setFieldIgnore(((FieldFilterB2) fields[fidx]).getRelateIdx(), fields);
                        } else {
                            fields[fidx] = new FieldFilterB1(crf.dimension, sortedDims);
                        }
                    } else if (CUBOID_FILTER_TYPE_W.equalsIgnoreCase(crf.type)) {
                        if (null != crf.dimension_relate && dimsMap.get(crf.dimension_relate.toLowerCase()) != null) {
                            fields[fidx] = new FieldFilterW2(crf.dimension, sortedDims, crf.dimension_relate);
                            setFieldIgnore(((FieldFilterW2) fields[fidx]).getRelateIdx(), fields);
                        } else {
                            fields[fidx] = new FieldFilterW1(crf.dimension, sortedDims);
                        }
                    } else {
                        LOG.error("Unknown cuboid filter type :" + crf.type);
                    }

                    if (fields[fidx] instanceof FieldFilter) {
                        FieldFilter ff = ((FieldFilter) fields[fidx]);
                        if (CUBOID_FILTER_SOURCE_C.equalsIgnoreCase(crf.source)) {
                            Object conf = confs.get(crf.value);
                            if (null == conf) {
                                throw new IOException("Cannot find config in report.def named " + crf.value);
                            }
                            if (conf instanceof List) {
                                ff.setList((List) conf);
                            } else if (conf instanceof Map) {
                                ff.setList((Map) conf);
                            } else if (conf instanceof Set) {
                                ff.setList((Set) conf);
                            } else {
                                throw new IOException("Unsupported configed data type " + conf.getClass() + " in filter conf");
                            }
                        } else if (CUBOID_FILTER_SOURCE_L.equalsIgnoreCase(crf.source)) {
                            ff.setList(crf.value, crf.split);
                        } else {
                            LOG.error("Unknown cuboid filter type :" + crf.type);
                            throw new IOException("Unknown cuboid filter type :" + crf.type);
                        }
                    }

                }
            }
        }

        return fields;
    }

    protected boolean setFieldIgnore(int fidx, Field[] fields) throws IOException {
        Field field_toset = fields[fidx];
        if (field_toset instanceof FieldBind) {
            for (Integer bidx : ((FieldBind) field_toset).bindFieds) {
                setFieldIgnore(bidx, fields);
            }
            LOG.warn("Field " + fidx + " is bind but also ignore!");
        } else if (field_toset instanceof FieldFilter) {
            LOG.warn("Field " + fidx + " is filter but also ignore!");
        }
        fields[fidx] = new FieldIgnore(fidx);
        return true;
    }

    protected boolean setFieldBind(int fidx, String fieldName, List<String> sortedDims, List<String> field_bind,
                                   Field[] fields) throws IOException {
        if (fields[fidx] instanceof FieldIgnore) {
            LOG.warn("Field " + fidx + " is ignore, will not be set as bind");
            return false;
        } else if (fields[fidx] instanceof FieldFilter) {
            LOG.warn("Field " + fidx + " is filter, will not be set as bind");
            return false;
        } else {
            fields[fidx] = new FieldBind(fieldName, sortedDims, field_bind);
        }
        return true;
    }

    private abstract class CuboidRule {

    }

    private class CuboidRuleIgnore extends CuboidRule {
        public List<String> field_ignore = new ArrayList<String>();

        public CuboidRuleIgnore(String fields) throws IOException {
            if (null != fields && fields.length() > 0) {
                String[] fields_array = fields.split(",");
                for (String field : fields_array) {
                    field_ignore.add(field);
                }
            } else {
                throw new IOException("Error definition in Cuboid ignore: fields is empty!");
            }
        }

        @Override
        public String toString() {
            return "CuboidRuleIgnore{" +
                    "field_ignore=" + field_ignore +
                    "} ";
        }
    }

    private class CuboidRuleBind extends CuboidRule {
        public List<String> field_bind = new ArrayList<String>();

        public CuboidRuleBind(String fields) throws IOException {
            if (null != fields && fields.length() > 0) {
                String[] fields_array = fields.split(",");
                for (String field : fields_array) {
                    field_bind.add(field);
                }
            } else {
                throw new IOException("Error definition in Cuboid bind: fields is empty!");
            }
        }

        @Override
        public String toString() {
            return "CuboidRuleBind{" +
                    "field_bind=" + field_bind +
                    "} ";
        }
    }

    private class CuboidRuleFilter extends CuboidRule {
        String dimension;
        String dimension_relate = null;
        String type;
        String source;
        String split = ",";
        String value;

        public CuboidRuleFilter(String dimension, String type, String source, String split, String value)
                throws IOException {
            if (dimension == null || dimension.length() == 0) {
                throw new IOException("Error definition in Cuboid filter: dimension is empty!");
            }
            if (null == type || (!"WList".equalsIgnoreCase(type) && !"BList".equalsIgnoreCase(type))) {
                throw new IOException("Error definition in Cuboid filter: type is illegal, should be WList or BList");
            }
            if (null == source || (!"list".equalsIgnoreCase(source) && !"config".equalsIgnoreCase(source))) {
                throw new IOException("Error definition in Cuboid filter: source is illegal, should be list or config");
            }
            if (null == value || value.length() == 0) {
                throw new IOException("Error definition in Cuboid filter: value is empty!");
            }

            String[] dimensions = dimension.split("\\.");
            if (dimensions.length == 2) {
                this.dimension = dimensions[1];
                dimension_relate = dimensions[0];
            } else {
                this.dimension = dimension;
            }
            this.type = type;
            this.source = source;
            this.split = (split == null || split.length() == 0) ? "," : split;
            this.value = value;
        }

        @Override
        public String toString() {
            return "CuboidRuleFilter{" +
                    "dimension='" + dimension + '\'' +
                    ", dimension_relate='" + dimension_relate + '\'' +
                    ", type='" + type + '\'' +
                    ", source='" + source + '\'' +
                    ", split='" + split + '\'' +
                    ", value='" + value + '\'' +
                    "} ";
        }
    }

    public void addCuboidRuleIgnore(String fields) throws IOException {
        this.cuboidRules.add(new CuboidRuleIgnore(fields));
    }

    public void addCuboidRuleBind(String fields) throws IOException {
        this.cuboidRules.add(new CuboidRuleBind(fields));
    }

    public void addCuboidRuleFilter(String dimension, String type, String source, String split, String value) throws IOException {
        this.cuboidRules.add(new CuboidRuleFilter(dimension, type, source, split, value));
    }

    @Override
    public String toString() {
        return "Cuboid{" +
                "cuboidRules=" + cuboidRules +
                '}';
    }
}
