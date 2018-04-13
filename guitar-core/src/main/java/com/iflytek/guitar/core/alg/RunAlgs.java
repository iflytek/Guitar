package com.iflytek.guitar.core.alg;

import com.iflytek.guitar.core.data.ReportKey;
import com.iflytek.guitar.core.data.ReportValue;
import com.iflytek.guitar.core.hpath.PathDef;
import com.iflytek.guitar.core.mr.ReportMRComb;
import com.iflytek.guitar.core.mr.ReportMRCube;
import com.iflytek.guitar.core.mr.input.InputFormatFactory;
import com.iflytek.guitar.core.mr.output.HdfsOutputFormat;
import com.iflytek.guitar.core.util.ConstantConf;
import com.iflytek.guitar.core.util.FileOpt;
import com.iflytek.guitar.share.avro.mapreduce.AvroJob;
import com.iflytek.guitar.share.avro.mapreduce.input.AvroPairInputFormat;
import com.iflytek.guitar.share.avro.mapreduce.output.AvroPairOutputFormat;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.SafeDate;
import com.iflytek.guitar.share.utils.UtilOper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * 同时运行的报表（A run unit），需要有相同的输入，相同的最小频度
 */
public class RunAlgs extends Thread {
    private Log LOG;

    /* read from config begin */
    public AlgDef.Frequence freq;
    public Date startDate;
    public Map<String, AlgDef> algs;
    public boolean forceUpdate = false;
    /* read from config end */

    public Configuration conf = null;

    public boolean isAddToOutputServer = false;
    private boolean isStarted = false;
    private boolean isFinished = false;

    private String errInfo = null;
    //private String uniqueCode = null;

    public boolean getIsStarted() {
        return isStarted;
    }

    public boolean getIsFinished() {
        return isFinished;
    }

    public String getErrInfo() {
        return errInfo;
    }

    public String getUniqueCode() {
        return ""
                + algs.keySet().iterator().next().replaceAll("\\[", "").replaceAll("]", "") + "_"
                + algs.keySet().size() + "_"
                + SafeDate.Date2Format(startDate, Constants.DATE_FORMAT_BASIC).replaceAll(":", "H") + "_"
                + freq.toString()
                + "";
    }

    /*
     * 解析参数: <algs> <start_time> <freq> <forceupdate> <-is_todb b>
	 * arg1: algs, 指定哪些报表需要运行,逗号分割
	 * arg2: start_time, 指定的时间戳
	 * arg3: freq, 指定报表的时间粒度, Hourly/Daily/Weekly/Monthly等
	 * arg4: forceupdate, 指定报表是否需要强制更新, false表示报表输出已经存在的话则不运行
	 * arg5和args6: -is_todb b, 指定是否入库，false表示报表输出到hdfs后不入库
	 */
    public RunAlgs(String[] args, Configuration config) throws Exception {
        if (null == args || args.length < 4) {
            throw new Exception("args error: <algs> <start_time> <freq> <forceupdate> <-is_todb b>\n"
                    + "algs: describe what reports need to run .\n"
                    + "start_time: describe the timestamp info for all reports.\n"
                    + "freq: describe the running period of reports.\n"
                    + "forceupdate: describe how to do when report's output is exist, false-not running when output is exist.\n"
                    + "-is_todb b: describe whether to db, default true-to db.\n");
        }

        this.conf = null == config ? new Configuration(config) : config;
//        this.uniqueCode = "[" + args[0] + "_" + args[1] + "_" + args[2] + "]";

        init(args[0], args[1], args[2], args[3]);

    }

    public RunAlgs(AlgDef algdef, Date startDate, AlgDef.Frequence freq, boolean forceUpdate, Configuration conf) {

        this.freq = freq;
        this.startDate = startDate;
        this.algs = new HashMap<String, AlgDef>();
        this.algs.put(algdef.alg_name, algdef);
        this.forceUpdate = forceUpdate;

        this.conf = null == conf ? new Configuration(conf) : conf;

        LOG = LogFactory.getLog(getUniqueCode());
    }

    public void init(String algs_s, String start_time_s, String freq_s, String force_s) throws Exception {
        String[] algs_list = algs_s.split(",");

        this.freq = AlgDef.Frequence.valueOf(freq_s);
        this.startDate = SafeDate.Format2Date(start_time_s.trim(), Constants.DATE_FORMAT_BASIC);
        if (null == algs_list || algs_list.length == 0) {
            throw new Exception("alg must not be empty!");
        }
        this.algs = new HashMap<String, AlgDef>();
        for (String alg : algs_list) {
            this.algs.put(alg, AlgDef.getAlgDefByName(alg, AlgDef.Frequence.valueOf(freq_s), conf));
        }
        this.forceUpdate = Boolean.valueOf(force_s);

        LOG = LogFactory.getLog(getUniqueCode());

        LOG.info("args: \n " +
                "algs=" + algs_s + "\n" +
                "start_time=" + start_time_s + "\n" +
                "freq=" + freq_s + "\n" +
                "forceupdate=" + force_s);

    }

    public AlgDef.Priority getPri() throws IOException {
        AlgDef.Priority pri = new AlgDef.Priority(-1, -1);
        for (AlgDef def : algs.values()) {
            if (def.priority.bigger(pri)) {
                pri = def.priority;
            }
        }

        return new AlgDef.Priority(pri.s, pri.w);
    }

    private int addInput(AvroJob job, List<String> inputs, FileSystem fs) throws IOException, ClassNotFoundException {
        if (null == inputs) {
            LOG.error("no inputs!!!");
            return Constants.RET_INT_ERROR;
        }

        // 多报表合并运行时，数据检查以最全的数据检查机制为准，no < part < all
        Input.CheckType biggestCheckType = Input.CheckType.no;
        for (AlgDef algDef : this.algs.values()) {
            Input.CheckType check_type = algDef.input.check_type;
            if (check_type.bigger(biggestCheckType)) {
                biggestCheckType = check_type;
            }
        }

        /* 解析通配符 */
        int totalInputNum = inputs.size();
        int existInputNum = 0;
        int ret = Constants.RET_INT_ERROR;
        for (String input : inputs) {
            List<Path> paths = FileOpt.parseWordcardDir(fs, input);
            if (null == paths || paths.size() <= 0) {
                continue;
            }

            existInputNum++;

            for (Path path : paths) {
                MultipleInputs.addInputPath(job, path, InputFormatFactory.get().get(this, job));
                if (Constants.RET_INT_ERROR == ret) {
                    ret = Constants.RET_INT_OK;
                }
                LOG.info("add input file[" + path.toString() + "] format["
                        + InputFormatFactory.get().get(this, job).getSimpleName() + "]");
            }
        }

        /* 检查输入数据的完整性,指的是时间数据的完整性，不考虑各个通配的情况 */
        if (Constants.VALUE_DATA_CHECK_ALL.equals(biggestCheckType.toString())) {
                /* 配置检查所有输入数据时, 当不是所有输入时间粒度的数据存在时, 报错 */
            if (existInputNum < totalInputNum) {
                LOG.error("existInputNum[" + existInputNum + "] < totalInputNum[" + totalInputNum + "]");
                return Constants.RET_INT_ERROR;
            }
        } else if (Constants.VALUE_DATA_CHECK_PART.equals(biggestCheckType.toString())) {
                /* 配置检查部分输入数据时, 当搜有输入时间粒度的数据不存在时, 报错 */
            if (0 == existInputNum) {
                LOG.error("existInputNum[" + existInputNum + "]");
                return Constants.RET_INT_ERROR;
            }
        }

        return ret;

    }

    @Override
    public void run() {
        /*
         * 中间存储目录生成规则和mr调度规则：
         * 主要涉及两个目录，
         * 1. 中间数据目录：report/interm/[freq]/[alg_name]    -- 用处不大，舍弃
         * 2. 结果数据目录：report/output/[freq]/[alg_name]
         *
         * 执行时，分以下四种情况
         * 1. 无cube、无filter
         *      input -> MRcomb -> 结果数据目录 -> Main入库
         * 2. 有cube、无filter
         *      input -> MRcomb -> mp目录 -> MRcube -> 结果数据目录 -> Main入库
         * 3. 无cube、有filter
         *      input -> MRcomb -> 结果数据目录 -> MR入库
         * 4. 有cube、有filter
         *      input -> MRcomb -> tmp目录 -> MRcube -> 结果数据目录 -> MR入库
         */
        isStarted = true;
        try {
            StringBuilder sb = new StringBuilder();
            boolean isfirst = true;
            for (String alg : algs.keySet()) {
                if (isfirst) {
                    isfirst = false;
                } else {
                    sb.append(",");
                }
                sb.append(alg);
            }
            conf.set(ConstantConf.ALG_LIST, sb.toString());
            conf.set(ConstantConf.CONF_TASK_TIME, SafeDate.Date2Format(startDate, Constants.DATE_FORMAT_BASIC));
            conf.set(ConstantConf.CONF_TASK_FREQ, freq.toString());

            // 读取中间数据
            if (isReadMidData()) {
                String tmpDir_comb_mid = SafeDate.Date2Format(new Date(), Constants.DATE_FORMAT_MSEC) + "_" + getUniqueCode() + "_comb_mid";
                String outputTmpDir_comb_mid = Constants.DIR_REPORT_DIR + "/" + Constants.DIR_REPORT_SUBDIR_TMP + "/" + tmpDir_comb_mid;
                if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
                    outputTmpDir_comb_mid = UtilOper.getBaseDirString(conf) + "/" + outputTmpDir_comb_mid;
                }
                LOG.info("Comb_mid tmp output : " + outputTmpDir_comb_mid);

                List<String> inputs = getInputs();
                Integer reduceNum = FileOpt.getCombReduceNum(inputs, 300L * 1024L * 1024L, algs.size(), conf);
                if (null == reduceNum) {
                    throw new IOException("Get Reduce Num failed!");
                }
                LOG.info("Reduce number by empirical calculation is " + reduceNum);

                runJobComb_mid(inputs, outputTmpDir_comb_mid, reduceNum);

                // 迁移结果数据
                String outputDir = Constants.DIR_REPORT_DIR;
                if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
                    outputDir = UtilOper.getBaseDirString(conf) + "/" + outputDir;
                }
                if (!UtilOper.install(conf, outputTmpDir_comb_mid, outputDir)) {
                    LOG.error("install return false");
                    throw new Exception(("install return false"));
                }

                // 删除缓存输出数据
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(outputTmpDir_comb_mid), true);

            } else { // 读取原始日志

                // job1, for combine
                String tmpDir_comb_org = SafeDate.Date2Format(new Date(), Constants.DATE_FORMAT_MSEC) + "_" + getUniqueCode() + "_comb_org";
                String outputTmpDir_comb_org = Constants.DIR_REPORT_DIR + "/" + Constants.DIR_REPORT_SUBDIR_TMP + "/" + tmpDir_comb_org;
                if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
                    outputTmpDir_comb_org = UtilOper.getBaseDirString(conf) + "/" + outputTmpDir_comb_org;
                }
                LOG.info("Comb tmp output : " + outputTmpDir_comb_org);

                List<String> inputs = getInputs();
                Integer reduceNum = FileOpt.getCombReduceNum(inputs, 300L * 1024L * 1024L, algs.size(), conf);
                if (null == reduceNum) {
                    throw new IOException("Get Reduce Num failed!");
                }
                LOG.info("Reduce number by empirical calculation is " + reduceNum);

                runJobComb_org(inputs, outputTmpDir_comb_org, reduceNum);

                // job2, for cube
                String tmpDir_cube = SafeDate.Date2Format(new Date(), Constants.DATE_FORMAT_MSEC) + "_" + getUniqueCode() + "_cube";
                String outputTmpDir_cube = Constants.DIR_REPORT_DIR + "/" + Constants.DIR_REPORT_SUBDIR_TMP + "/" + tmpDir_cube;
                if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
                    outputTmpDir_cube = UtilOper.getBaseDirString(conf) + "/" + outputTmpDir_cube;
                }
                LOG.info("Cube tmp output : " + outputTmpDir_cube);

                runJobCube(outputTmpDir_comb_org, outputTmpDir_cube, reduceNum);

                // 迁移结果数据
                String outputDir = Constants.DIR_REPORT_DIR;
                if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
                    outputDir = UtilOper.getBaseDirString(conf) + "/" + outputDir;
                }
                if (!UtilOper.install(conf, outputTmpDir_cube, outputDir)) {
                    LOG.error("install return false");
                    throw new Exception(("install return false"));
                }

                // 删除缓存输出数据
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(outputTmpDir_comb_org), true);
                fs.delete(new Path(outputTmpDir_cube), true);
            }

            LOG.info("job[" + getUniqueCode() + "] running success!");

        } catch (Exception ex) {
            this.errInfo = ex.getMessage();
            ex.printStackTrace();
        }

        isFinished = true;

    }

    public boolean isReadMidData() {
        if (algs.size() > 0) {
            return algs.entrySet().iterator().next().getValue().isReadMidData(freq);
        }

        return false;
    }

    public List<String> getInputs() throws IOException {
        List<String> inputs = new ArrayList<String>();
        /*
         * 报表可以同时跑的前提是报表有相同的输入数据
         * （对于读取中间数据的高频度报表，这里不做合并处理，即此种情况一定是每个报表单独跑的）
         * 因此同时跑的算法中，getInputs应该返回相同的输入目录
         */
        if (algs.size() > 0) {
            inputs = algs.entrySet().iterator().next().getValue().getInputPath(freq, startDate, conf);
        }

        return inputs;
    }

    private void runJobComb_mid(List<String> inputs, String output, Integer reduceNum)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf_comb = null == conf ? new Configuration(conf) : conf;

        AvroJob job = AvroJob.getAvroJob(conf_comb);
        job.setJobName(this.getUniqueCode() + "_comb_mid");
        job.setNumReduceTasks(reduceNum);

        FileSystem fs = FileSystem.get(conf_comb);
        for (String input : inputs) {
            Path inputPath = new Path(input);
            if (fs.exists(inputPath)) {
                AvroPairInputFormat.addInputPath(job, inputPath);
                LOG.info("add input file: " + inputPath);
            } else {
                LOG.warn("skip not exist file: " + inputPath);
            }
        }

        job.setInputFormatClass(AvroPairInputFormat.class);
        job.setMapperClass(ReportMRComb.M_mid.class);
        // read from interm, do not setCombinerClass!!
        job.setReducerClass(ReportMRComb.R.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(HdfsOutputFormat.class);

        job.setOutputKeyClass(ReportKey.class);
        job.setOutputValueClass(ReportValue.class);

        if (!job.waitForCompletion(true)) {
            LOG.error("job.waitForCompletion return false");
            throw new IOException(("JobComb_mid.waitForCompletion return false"));
        }
    }

    private void runJobComb_org(List<String> inputs, String output, Integer reduceNum)
            throws IOException, ClassNotFoundException, InterruptedException, SQLException {

        Configuration conf_comb = null == conf ? new Configuration(conf) : conf;

        readConfigInternData(conf_comb);

        FileSystem fs = FileSystem.get(conf_comb);

        AvroJob job = AvroJob.getAvroJob(conf_comb);
        job.setJobName(this.getUniqueCode() + "_comb_org");
        if (conf_comb.getInt(ConstantConf.COMB_REDUCE_NUM, 0) != 0) {
            job.setNumReduceTasks(conf_comb.getInt(ConstantConf.COMB_REDUCE_NUM, 0));
        } else {
            job.setNumReduceTasks(reduceNum);
        }

        if (Constants.RET_INT_OK != addInput(job, inputs, fs)) {
            throw new IOException(("JobComb_org.inputs error"));
        }

        job.setMapperClass(ReportMRComb.M_org.class);
        job.setCombinerClass(ReportMRComb.C.class);
        job.setReducerClass(ReportMRComb.R.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(AvroPairOutputFormat.class);

//        job.setMapOutputKeyClass(ReportKey.class);
//        job.setMapOutputValueClass(ReportValue.class);
        job.setOutputKeyClass(ReportKey.class);
        job.setOutputValueClass(ReportValue.class);

        if (!job.waitForCompletion(true)) {
            LOG.error("job.waitForCompletion return false");
            throw new IOException(("JobComb_org.waitForCompletion return false"));
        }
    }

    private void runJobCube(String input, String output, Integer reduceNum)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf_cube = null == conf ? new Configuration(conf) : conf;
        // mapreduce.map.memory.mb = 4056
        // mapreduce.map.java.opts = -Djava.net.preferIPv4Stack=true -Xmx3076m
        String map_mb = conf_cube.get("mapreduce.map.memory.mb.cube", "4096");
        conf_cube.set("mapreduce.map.memory.mb", map_mb);
        String java_opts = conf_cube.get("mapreduce.map.java.opts.cube", "-Djava.net.preferIPv4Stack=true -Xmx4096m");
        conf_cube.set("mapreduce.map.java.opts", java_opts);

        AvroJob job = AvroJob.getAvroJob(conf_cube);
        job.setJobName(this.getUniqueCode() + "_cube");
        if (conf_cube.getInt(ConstantConf.CUBE_REDUCE_NUM, 0) != 0) {
            job.setNumReduceTasks(conf_cube.getInt(ConstantConf.CUBE_REDUCE_NUM, 0));
        } else {
            job.setNumReduceTasks(reduceNum);
        }

        job.setInputFormatClass(AvroPairInputFormat.class);
        AvroPairInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(ReportMRCube.M.class);
        job.setReducerClass(ReportMRCube.R.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(HdfsOutputFormat.class);

//        job.setMapOutputKeyClass(ReportKey.class);
//        job.setMapOutputValueClass(ReportValue.class);
        job.setOutputKeyClass(ReportKey.class);
        job.setOutputValueClass(ReportValue.class);

        if (!job.waitForCompletion(true)) {
            LOG.error("job.waitForCompletion return false");
            throw new IOException(("JobCube.waitForCompletion return false"));
        }
    }

    private void readConfigInternData(Configuration conf) throws IOException, SQLException {
        for (AlgDef algdef : this.algs.values()) {
            if (null != algdef.aconfigs && algdef.aconfigs.size() > 0) {
                for (Aconfig aconfig : algdef.aconfigs) {
                    Path conf_intern = PathDef.get(PathDef.CONFIG_INTER).
                            parseDate(this.startDate).parseFreq(freq).parseID(aconfig.id).toPath();
                    if (conf.get(Constants.GUITAR_BASE_DIR) != null) {
                        conf_intern = new Path(UtilOper.getBaseDir(conf), conf_intern);
                    }
                    aconfig.read2File(conf_intern, conf);
                }
            }
        }
    }


}
