package com.iflytek.guitar.core.alg;

import com.iflytek.guitar.core.mr.ReportObject;
import com.iflytek.guitar.core.util.ScriptOper;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.UtilOper;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class AlgDef {
    private static Log LOG;
    private static final String COMMENT = "comment";

    private Configuration conf;

    public String alg_name;

    public Input input;

    public Output output;
    public Map<Frequence, String> customTableName;

    public List<Aconfig> aconfigs;

    public List<Dimension> dimensions = new ArrayList<Dimension>();
    public List<Target> targets = new ArrayList<Target>();

    public Frequence minFreq;
    public Frequence mainFreq; // 非xml配置，主函数传来

    public Priority priority = new Priority(0, 0);

    public Cuboid cuboid = new Cuboid();

    // TODO 子报表，预留功能
    public List<SubReport> sub_reports = new ArrayList<SubReport>();

    //    public GroovyObject parseGroovyObject;
    public ScriptOper parseGroovyObject;

    public static AlgDef getAlgDefByName(String alg_name, Frequence mainFreq, Configuration conf)
            throws IOException, ParserConfigurationException, SAXException {
        AlgDef algdef = new AlgDef();
        LOG = LogFactory.getLog(AlgDef.class.getSimpleName() + "(" + alg_name + ")");
        algdef.alg_name = alg_name;
        algdef.mainFreq = mainFreq;
        algdef.conf = conf;

        FileSystem fs = FileSystem.get(conf);
        Path path = algdef.getAlgDefPath(conf);
        /* 打开xml文件，建立DOC-xml模型 */
        InputStream dataFileInput = fs.open(path);
        DocumentBuilder docBD = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = docBD.parse(dataFileInput);

        // Parse input
        algdef.input = new Input();
        NodeList nl_input = document.getElementsByTagName("input").item(0).getChildNodes();
        for (int i = 0; i < nl_input.getLength(); i++) {
            Node node = nl_input.item(i);
            // #comment or nodetype=8 means : <!-- comment -->
            if (node.getNodeType() == 3 || COMMENT.equals(node.getNodeName()) || "#comment".equals(node.getNodeName())) {
                // 3 is type #text, no use at all
            } else if ("path".equals(node.getNodeName())) {
                algdef.input.paths.add(getNodeTextValue(node));
            } else if ("data_type".equals(node.getNodeName())) {
                algdef.input.data_type = Input.DataType.valueOf(getNodeTextValue(node));
            } else if ("file_type".equals(node.getNodeName())) {
                algdef.input.file_type = Input.FileType.valueOf(getNodeTextValue(node));
            } else if ("parquet_schema_class".equals(node.getNodeName())) { //如果读入文件是parquet格式，可配置类信息和子字段
                algdef.input.parquet_schema_class = getNodeTextValue(node);
            } else if ("parquet_schema_subfields".equals(node.getNodeName())) { //如果读入文件是parquet格式，可配置类信息和子字段
                if (null == algdef.input.parquet_schema_subfields) {
                    algdef.input.parquet_schema_subfields = new HashSet<String>();
                }
                String[] subfields = getNodeTextValue(node).split(",");
                for (String subfield : subfields) {
                    algdef.input.parquet_schema_subfields.add(subfield);
                }
            } else if ("check".equals(node.getNodeName())) {
                algdef.input.check_type = Input.CheckType.valueOf(getNodeTextValue(node));
            } else {
                LOG.warn("Unknown node name of " + node.getNodeName() + " in input");
            }
        }

        // parse output
        algdef.output = new Output();
        NodeList nl_output = document.getElementsByTagName("output").item(0).getChildNodes();
        for (int i = 0; i < nl_output.getLength(); i++) {
            Node node = nl_output.item(i);
            if (node.getNodeType() == 3 || COMMENT.equals(node.getNodeName()) || node.getNodeType() == 8) {
                // 3 is type #text, no use at all
            } else if ("type".equals(node.getNodeName())) {
                algdef.output.otype = Output.OType.valueOf(getNodeTextValue(node));
            } else if ("param".equals(node.getNodeName())) {
                NamedNodeMap param = node.getAttributes();
                Map<String, String> param_map = new HashMap<String, String>();
                for (int j = 0; j < param.getLength(); j++) {
                    param_map.put(param.item(j).getNodeName(), param.item(j).getNodeValue());
                }
                algdef.output.setParam(param_map);
            } else if ("FreqTableName".equals(node.getNodeName())) {
                String[] freqTableNames = getNodeTextValue(node).split(",");
                Map<Frequence, String> map = new HashMap<Frequence, String>();
                for (String freqTableName : freqTableNames) {
                    Frequence freq = Frequence.valueOf(freqTableName.split(":")[0]);
                    String tableName = freqTableName.split(":")[1];
                    map.put(freq, tableName);
                }
                algdef.customTableName = map;
            } else if ("filter".equals(node.getNodeName())) { // TODO 入库时过滤部分数据，预留功能
                NamedNodeMap filter = node.getAttributes();
                String key = filter.getNamedItem("key").getNodeValue();
                String param = filter.getNamedItem("param").getNodeValue();
                String type = filter.getNamedItem("type").getNodeValue();
                algdef.output.addFilter(key, type, param);
            } else {
                LOG.warn("Unknown node name of " + node.getNodeName() + "in output");
            }
        }

        // parse config
        algdef.aconfigs = new ArrayList<Aconfig>();
        NodeList nl_config = document.getElementsByTagName("config");
        if (null != nl_config) {
            for (int iConf = 0; iConf < nl_config.getLength(); iConf++) {
                Aconfig aconfig = new Aconfig();
                Node conf_item = nl_config.item(iConf);
                aconfig.type = conf_item.getAttributes().getNamedItem("type").getNodeValue();
                aconfig.id = conf_item.getAttributes().getNamedItem("id").getNodeValue();

                NodeList nl_config_item = conf_item.getChildNodes();
                for (int i = 0; i < nl_config_item.getLength(); i++) {
                    Node node = nl_config_item.item(i);
                    if (node.getNodeType() == 3 || COMMENT.equals(node.getNodeName()) || node.getNodeType() == 8) {
                        // 3 is type #text, no use at all
                    } else if ("sql".equals(node.getNodeName())) {
                        aconfig.sql = getNodeTextValue(node);
                    } else if ("param".equals(node.getNodeName())) {
                        NamedNodeMap param = node.getAttributes();
                        Map<String, String> param_map = new HashMap<String, String>();
                        for (int j = 0; j < param.getLength(); j++) {
                            param_map.put(param.item(j).getNodeName(), param.item(j).getNodeValue());
                        }
                        aconfig.setParam(param_map);
                    } else if ("path".equals(node.getNodeName())) {
                        aconfig.path = getNodeTextValue(node);
                    } else {
                        LOG.warn("Unknown node name of " + node.getNodeName() + " in output");
                    }
                }
                algdef.aconfigs.add(aconfig);
            }
        }

        // parse report
        NodeList nl_report = document.getElementsByTagName("report").item(0).getChildNodes();
        for (int i = 0; i < nl_report.getLength(); i++) {
            Node node = nl_report.item(i);
            if (node.getNodeType() == 3 || COMMENT.equals(node.getNodeName()) || node.getNodeType() == 8) {
                // 3 is type #text, no use at all
            }
            // -- parse dimension
            else if ("dimension".equals(node.getNodeName())) {
                NodeList nl_dim = node.getChildNodes();
                for (int j = 0; j < nl_dim.getLength(); j++) {
                    if (nl_dim.item(j).getNodeType() == 3 || COMMENT.equals(nl_dim.item(j).getNodeName()) || nl_dim.item(j).getNodeType() == 8) {
                        // 3 is type #text, no use at all
                    } else {
                        String value = null;
                        if (null != nl_dim.item(j).getAttributes().getNamedItem("value")) {
                            value = nl_dim.item(j).getAttributes().getNamedItem("value").getNodeValue();
                        }
                        algdef.addDimension(nl_dim.item(j).getNodeName(), value);
                    }
                }
            }
            // -- parse target
            else if ("target".equals(node.getNodeName())) {
                NodeList nl_tag = node.getChildNodes();
                for (int j = 0; j < nl_tag.getLength(); j++) {
                    if (nl_tag.item(j).getNodeType() != 3 && !COMMENT.equals(nl_tag.item(j).getNodeName()) && nl_tag.item(j).getNodeType() != 8) {
                        // 3 is type #text, no use at all
                        String name = nl_tag.item(j).getNodeName();
                        String type = nl_tag.item(j).getAttributes().getNamedItem("type").getNodeValue();
                        String param = null;
                        if (null != nl_tag.item(j).getAttributes().getNamedItem("param")) {
                            param = nl_tag.item(j).getAttributes().getNamedItem("param").getNodeValue();
                        }
                        String value = null;
                        if (null != nl_tag.item(j).getAttributes().getNamedItem("value")) {
                            value = nl_tag.item(j).getAttributes().getNamedItem("value").getNodeValue();
                        }
                        algdef.addTarget(name, type, value, param);
                    }
                }
            }
            // -- parse minFreq
            else if ("minFreq".equals(node.getNodeName())) {
                algdef.minFreq = Frequence.valueOf(getNodeTextValue(node));
            }
            // -- parse priority
            else if ("priority".equals(node.getNodeName())) {
                algdef.priority.set(getNodeTextValue(node));
            }
            // -- parse cuboid
            else if ("cuboid".equals(node.getNodeName())) {
                NodeList nl_sre = node.getChildNodes();
                for (int j = 0; j < nl_sre.getLength(); j++) {
                    if (nl_sre.item(j).getNodeType() != 3 && !COMMENT.equals(nl_sre.item(j).getNodeName()) && nl_sre.item(j).getNodeType() != 8) {
                        // 3 is type #text, no use at all
                        String name = nl_sre.item(j).getNodeName();
                        String value = getNodeTextValue(nl_sre.item(j));
                        if ("ignore".equalsIgnoreCase(name)) {
                            algdef.cuboid.addCuboidRuleIgnore(value);
                        } else if ("bind".equalsIgnoreCase(name)) {
                            algdef.cuboid.addCuboidRuleBind(value);
                        } else if ("filter".equalsIgnoreCase(name)) {
                            String dimension = null, type = null, source = null, split = null;
                            if (null != nl_sre.item(j).getAttributes().getNamedItem("dimension")) {
                                dimension = nl_sre.item(j).getAttributes().getNamedItem("dimension").getNodeValue();
                            }
                            if (null != nl_sre.item(j).getAttributes().getNamedItem("type")) {
                                type = nl_sre.item(j).getAttributes().getNamedItem("type").getNodeValue();
                            }
                            if (null != nl_sre.item(j).getAttributes().getNamedItem("source")) {
                                source = nl_sre.item(j).getAttributes().getNamedItem("source").getNodeValue();
                            }
                            if (null != nl_sre.item(j).getAttributes().getNamedItem("split")) {
                                split = nl_sre.item(j).getAttributes().getNamedItem("split").getNodeValue();
                            }
                            algdef.cuboid.addCuboidRuleFilter(dimension, type, source, split, value);
                        }
                    }
                }
                System.out.println(algdef.cuboid.toString());
            }
            // -- parse sub-report
            else if ("sub-report".equals(node.getNodeName())) {
                NodeList nl_sre = node.getChildNodes();
                for (int j = 0; j < nl_sre.getLength(); j++) {
                    if (nl_sre.item(j).getNodeType() != 3 && !COMMENT.equals(nl_sre.item(j).getNodeName()) && nl_sre.item(j).getNodeType() != 8) {
                        // 3 is type #text, no use at all
                        String name = nl_sre.item(j).getNodeName();
                        String type = nl_sre.item(j).getAttributes().getNamedItem("type").getNodeValue();
                        algdef.addSubReport(name, type);
                    }
                }
            }
            // -- unknown node
            else {
                LOG.warn("unknown node " + node.getNodeName());
            }
        }

        // get parse groovy object
        Path scriptPath = algdef.getAlgParsePath(conf);
        if (fs.exists(scriptPath)) {
            FSDataInputStream fsInputStream = fs.open(scriptPath);
            ClassLoader parent = algdef.getClass().getClassLoader();
            GroovyClassLoader loader = new GroovyClassLoader(parent);
            try {
                Class gclass = loader.parseClass(fsInputStream, scriptPath.getName());
                //将对象实例化并且强制转换为GroovyObject对象
//                GroovyObject groovyObject = (GroovyObject) gclass.newInstance();
                ScriptOper groovyObject = (ScriptOper) gclass.newInstance();
                algdef.parseGroovyObject = groovyObject;
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new IOException("get parse groovy[" + scriptPath.toString() + "] faild: " + ex.getMessage());
            } finally {
                if (null != fsInputStream) {
                    fsInputStream.close();
                }
            }
        }

        // get config custom groovy object
        if (null == ScriptOper.CUSTOBJECT) {
            Path customScriptPath = algdef.getCustomScriptPath(conf);
            if (fs.exists(customScriptPath)) {
                FSDataInputStream fsInputStream = fs.open(customScriptPath);
//                ClassLoader parent = ClassLoader.getSystemClassLoader();
                ClassLoader parent = algdef.getClass().getClassLoader();
                GroovyClassLoader loader = new GroovyClassLoader(parent);
                try {
                    Class gclass = loader.parseClass(fsInputStream, customScriptPath.getName());
                    //将对象实例化
                    ScriptOper.setCustomObject(gclass.newInstance());
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.warn("load custom-groovy script[" + customScriptPath.toString() + "] exception");
                } finally {
                    if (null != fsInputStream) {
                        fsInputStream.close();
                    }
                }
            }
        }

        return algdef;
    }

    public static String getNodeTextValue(Node node) {
        Node n = node.getChildNodes().item(0);
        return n.getNodeValue().trim();
    }

    public class Dimension {

        public String name;
        public String value;

        public Dimension(String n, String v) {
            name = n;
            value = v;
        }

        @Override
        public String toString() {
            return "\nDimension{" +
                    "name='" + name + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    public void addDimension(String n, String v) {
        this.dimensions.add(new Dimension(n, v));
    }

    public class Target {

        public String name;
        public AlgTargetType type;
        public String param;
        public String value;

        public Target(String n, String t, String v, String p) {
            name = n;
            type = AlgTargetType.valueOf(t);
            param = p;
            value = v;
        }

        @Override
        public String toString() {
            return "\nTarget{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", param='" + param + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    public enum AlgTargetType {
        SUM,          //求和
        MAX,          //最大值
        MIN,          //最小值
        AVG,          //均值
        VALUEDIST,   //取值分布
        RANGEDIST,   //范围分布
        UVBITMAP,    //用bitmap算法计算基数
        UV_HYPERLOGLOG,      //用hyperloglog算法计算基数
        UV_HYPERLOGLOGPLUS, //用hyperloglogplus算法计算基数

        PV,           //=sum(1)，暂不支持
        UV,           //去重后个数，像用户数指标，暂不支持
        UV_ACC,       //累计UV，使用场景较少，暂不支持
        PERCENTDIST  //百分比分布，暂不支持
    }

    public void addTarget(String n, String t, String v, String p) {
        this.targets.add(new Target(n, t, v, p));
    }

    public enum Frequence {
        Hourly,
        Daily,
        Weekly,         //自然周
        Monthly,        //自然月
        Thirtydays,     //30天
        Quarterly,      //季度
        HalfYearly,     //半年
        Yearly;

        public Date getEndDate(Date start) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(start);
            switch (this) {
                case Hourly:
                    cal.add(Calendar.HOUR, 1);
                    break;
                case Daily:
                    cal.add(Calendar.DAY_OF_YEAR, 1);
                    break;
                case Weekly:
                    cal.add(Calendar.WEEK_OF_YEAR, 1);
                    break;
                case Monthly:
                    cal.add(Calendar.MONTH, 1);
                    break;
                case Thirtydays:
                    cal.add(Calendar.DAY_OF_YEAR, 30);
                    break;
                case Quarterly:
                    cal.add(Calendar.MONTH, 3);
                    break;
                case HalfYearly:
                    cal.add(Calendar.MONTH, 6);
                    break;
                case Yearly:
                    cal.add(Calendar.YEAR, 1);
                    break;
            }
            return cal.getTime();
        }

        public boolean bigger(Frequence o) {
            return this.compareTo(o) > 0;
        }

        public Frequence getChildFreq() throws IOException {
            switch (this) {
                case Hourly:
                    throw new IOException("Hourly has no child Freq");
                case Daily:
                    return Hourly;
                case Weekly:
                case Monthly:
                case Thirtydays:
                    return Daily;
                case Quarterly:
                case HalfYearly:
                case Yearly:
                    return Monthly;
                default:
                    throw new IOException("Unknown freq of " + this);
            }
        }

        public static void add(Calendar cal, Frequence freq, int unit) {
            switch (freq) {
                case Hourly:
                    cal.add(Calendar.HOUR, unit);
                    break;
                case Daily:
                    cal.add(Calendar.DAY_OF_YEAR, unit);
                    break;
                case Weekly:
                    cal.add(Calendar.WEEK_OF_YEAR, unit);
                    break;
                case Monthly:
                    cal.add(Calendar.MONTH, unit);
                    break;
                case Thirtydays:
                    cal.add(Calendar.DAY_OF_YEAR, 30 * unit);
                    break;
                case Quarterly:
                    cal.add(Calendar.MONTH, 3 * unit);
                    break;
                case HalfYearly:
                    cal.add(Calendar.MONTH, 6 * unit);
                    break;
                case Yearly:
                    cal.add(Calendar.YEAR, unit);
                    break;
            }
        }
    }

    public static class Priority {
        public int s = 0;  // 强优先级
        public int w = 0;  // 弱优先级

        public Priority() {
        }

        public Priority(int s, int w) {
            this.s = s;
            this.w = w;
        }

        public Priority(String p) throws IOException {
            this.set(p);
        }

        public void set(String p) throws IOException {
            if (null == p) {
                throw new IOException("Priority String is Empty!");
            }
            String[] ps = p.split("\\.");
            if (2 != ps.length) {
                throw new IOException("Priority String is illegal :" + p + ", should be n.m");
            }

            try {
                s = Integer.parseInt(ps[0]);
                w = Integer.parseInt(ps[1]);
            } catch (NumberFormatException ex) {
                ex.printStackTrace();
                throw new IOException("Priority String is illegal :" + p + ", should be n.m");
            }
        }

        public boolean bigger(Priority o) throws IOException {
            if (null == o) {
                throw new IOException("Priority is null when compare");
            }
            if (s != o.s) {
                return (s > o.s);
            } else {
                return (w > o.w);
            }
        }

        public boolean equals(Priority o) {
            if (null == o) {
                return false;
            } else {
                return (s == o.s && w == o.w);
            }
        }

        @Override
        public String toString() {
            return s + "." + w;
        }

    }

    public class SubReport {

        public String name;
        public SubReportType type;

        public SubReport(String n, String v) {
            name = n;
            type = SubReportType.valueOf(v);
        }

        @Override
        public String toString() {
            return "\nSubReport{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    '}';
        }
    }

    public enum SubReportType {
        list,
        value,
        bitmap
    }

    public void addSubReport(String n, String v) {
        sub_reports.add(new SubReport(n, v));
    }

    public Path getAlgDefPath(Configuration conf) {
        Path path = new Path("guitar/report_def/" + alg_name.replaceAll("\\.", "/") + "/report.def.xml");
        if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
            return new Path(UtilOper.getBaseDir(conf), path);
        }

        return path;
    }

    public Path getAlgParsePath(Configuration conf) {
        Path path = new Path("guitar/report_def/" + alg_name.replaceAll("\\.", "/") + "/parse.groovy");
        if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
            return new Path(UtilOper.getBaseDir(conf), path);
        }

        return path;
    }

    public Path getAlgAliasPath(Configuration conf) {
        Path path = new Path("guitar/report_def/" + alg_name.replaceAll("\\.", "/") + "/exportAliasName");
        if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
            return new Path(UtilOper.getBaseDir(conf), path);
        }

        return path;
    }

    public Path getCustomScriptPath(Configuration conf) {
        Path path = new Path("guitar/report_def/public/custom.groovy");
        if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
            return new Path(UtilOper.getBaseDir(conf), path);
        }

        return path;
    }

    public List<String> getInputPath(AlgDef.Frequence freq, Date startDate, Configuration conf)
            throws IOException {
        List<String> paths = null;

        if (freq == minFreq) {
            paths = input.getPathOrigin(freq, startDate);
        } else {
            paths = getMidInputPath(freq, startDate, conf);
        }

        return paths;
    }

    private List<String> getMidInputPath(AlgDef.Frequence freq, Date startDate, Configuration conf)
            throws IOException {
        if (!freq.bigger(minFreq)) {
            throw new IOException("get middle input path failed, " + freq + " is not bigger than " + minFreq);
        }

        List<String> paths = new ArrayList<String>();

        Date dt_end = freq.getEndDate(startDate);
        Calendar cal_beg = Calendar.getInstance();
        Calendar cal_end = Calendar.getInstance();
        cal_beg.setTime(startDate);
        cal_end.setTime(dt_end);

        Frequence freq_child = freq.getChildFreq();
        while (cal_beg.before(cal_end)) {
            String path_child = getOutPath(freq_child, cal_beg.getTime(), conf);
            paths.add(path_child);

            Frequence.add(cal_beg, freq_child, 1);
        }
        return paths;
    }

    public String getOutPath(AlgDef.Frequence freq, Date startDate) {
        String dateDir = UtilOper.getDateDir(freq.toString(), startDate);
        String path_out = Constants.DIR_REPORT_DIR + "/" + Constants.DIR_REPORT_SUBDIR_OUTPUT + "/"
                + UtilOper.getReportDir(freq.toString(), alg_name, dateDir);
        return path_out;
    }

    public String getOutPath(AlgDef.Frequence freq, Date startDate, Configuration conf) {
        if (conf.get(Constants.GUITAR_BASE_DIR) == null) {
            return getOutPath(freq, startDate);
        }

        return UtilOper.getBaseDirString(conf)  + "/" + getOutPath(freq, startDate);
    }

    public ReportObject getReportObject(String inputFile, Date taskTime, AlgDef.Frequence freq, Configuration conf)
            throws IOException {
        ReportObject ro;
        if (null == parseGroovyObject) {
            ro = new ReportObject.ReportObjectConf(this);
        } else {
            ro = new ReportObject.ReportObjectGroovy(this, inputFile, taskTime, freq, conf);
        }

        return ro;
    }

    public List<String> getSortedDimensionNameList() {
        List<String> ret = new ArrayList<String>(this.dimensions.size());
        for (AlgDef.Dimension di : dimensions) {
            ret.add(di.name);
        }
        Collections.sort(ret);

        return ret;
    }

    public List<String> getSortedTargetNameList() {
        List<String> ret = new ArrayList<String>(this.targets.size());
        for (AlgDef.Target tg : targets) {
            ret.add(tg.name);
        }
        Collections.sort(ret);

        return ret;
    }

    public String getSimpleAlgName() {
        int idx = alg_name.lastIndexOf('.');
        if (-1 != idx) {
            return alg_name.substring(idx + 1);
        }
        return alg_name;
    }

    public boolean isReadMidData(AlgDef.Frequence freq) {
        if (freq == minFreq) {
            return false;
        }
        return true;
    }

    // 检查报表配置是否正确
    public void check() throws Exception {
        if (null == alg_name) {
            throw new Exception("Configure Alg Error: alg name is Empty");
        }
        input.check();
        output.check();
        if (null == dimensions || dimensions.isEmpty()) {
            throw new Exception("Configure Alg Error: dimensions is Empty");
        }
        if (null == targets || targets.isEmpty()) {
            throw new Exception("Configure Alg Error: targets is Empty");
        }
        if (null == minFreq) {
            throw new Exception("Configure Alg Error: minFreq is Empty");
        }
    }

    @Override
    public String toString() {
        return "AlgDef{" +
                "\nalg_name='" + alg_name + '\'' +
                "\n, input=" + input +
                "\n, output=" + output +
                "\n, dimensions=" + dimensions +
                "\n, targets=" + targets +
                "\n, mainFreq=" + mainFreq +
                "\n, minFreq=" + minFreq +
                "\n, sub_reports=" + sub_reports +
                "\n, cuboid=" + cuboid +
                "\n, parseGroovyObject=" + parseGroovyObject +
                "\n}";
    }

}
