package com.iflytek.guitar.core.alg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Output {
    public OType otype;
    public OParam oparam;
    public List<OFilter> filters = new ArrayList<OFilter>();

    public void check() throws Exception {
        if (null == otype) {
            throw new Exception("Configure Output Error: type is Empty");
        }
        if (null == oparam) {
            throw new Exception("Configure Output Error: param is Empty");
        }
    }

    public enum OType {
        mysql,
        phoenix,
        redis,
        mongodb
    }

    public interface OParam {
    }

    public class OFilter {
        public String key;
        public FilterType type;
        public String param;

        @Override
        public String toString() {
            return "OFilter{" +
                    "key='" + key + '\'' +
                    ", type=" + type +
                    ", param='" + param + '\'' +
                    '}';
        }
    }

    public enum FilterType {
        Top
    }

    public void addFilter(String key, String type, String param) {
        OFilter of = new OFilter();
        of.key = key;
        of.type = FilterType.valueOf(type);
        of.param = param;

        filters.add(of);
    }

    public void setParam(Map<String, String> params) throws IOException {
        switch (otype) {
            case mongodb:
            case mysql:
                oparam = new OParamImpl();
                ((OParamImpl) oparam).ip = params.get("ip");
                ((OParamImpl) oparam).port = Integer.parseInt(params.get("port"));
                ((OParamImpl) oparam).user = params.get("user");
                ((OParamImpl) oparam).passwd = params.get("passwd");
                ((OParamImpl) oparam).dbName = params.get("dbName");
                break;
            case phoenix:
                oparam = new OParamImpl();
                ((OParamImpl) oparam).ip = params.get("ip");
                break;
            case redis:
                oparam = new OParamImpl();
                ((OParamImpl) oparam).cluster = params.get("cluster");
                break;
            default:
                throw new IOException("Unknown Param type of " + otype);
        }
    }

    public static class OParamImpl implements OParam {
        public String ip;
        public int port;
        public String cluster;
        public String user;
        public String passwd;
        public String dbName;
        // public String tableName; tableName= algName + freq

        @Override
        public String toString() {
            return "OParamImpl{" +
                    "ip='" + ip + '\'' +
                    ", port=" + port +
                    ", cluster=" + cluster +
                    ", user='" + user + '\'' +
                    ", passwd='" + passwd + '\'' +
                    ", dbName='" + dbName + '\'' +
                    //", tableName='" + tableName + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "Output{" +
                "\notype=" + otype +
                "\n, oparam=" + oparam +
                "\n, filters=" + filters +
                '}';
    }
}
