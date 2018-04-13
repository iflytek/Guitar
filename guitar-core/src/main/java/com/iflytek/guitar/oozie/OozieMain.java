package com.iflytek.guitar.oozie;

import com.iflytek.guitar.share.avro.mapreduce.AvroJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;

public abstract class OozieMain extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(OozieMain.class);

    public static final String OOZIE_JAVA_MAIN_CAPTURE_OUTPUT_FILE = "oozie.action.output.properties";
    public static final String OOZIE_JAVA_MAIN_NEW_JOBID_FILE = "oozie.action.newId.properties";
    public static final String OOZIE_JAVA_MAIN_CONF_FILE = "oozie.action.conf.xml";

    static Properties cp = new Properties();

    @Override
    public int run(String[] args) throws Exception {
        loadOozieConf();
        return 0;
    }

    public boolean runJob(AvroJob job) throws IOException, InterruptedException, ClassNotFoundException {
        job.submit();
        String idf = System.getProperty(OOZIE_JAVA_MAIN_NEW_JOBID_FILE);
        if (idf != null) {
            File idFile = new File(idf);
            String jobId = job.getJobID().toString();
            LOG.info("Save the Job ID:" + jobId + " to the file:" + idf);
            Properties props = new Properties();
            props.setProperty("id", jobId);
            OutputStream os = new FileOutputStream(idFile);
            props.store(os, "Store Job ID");
            os.close();
        }
        return job.waitForCompletion(true);
    }

    public void loadOozieConf() {
        if (System.getProperty(OOZIE_JAVA_MAIN_CONF_FILE) != null) {
            getConf().addResource(new Path("file:///", System.getProperty(OOZIE_JAVA_MAIN_CONF_FILE)));
        }
    }

    public void putActionData(String key, String value) {
        cp.put(key, value);
    }

    public void setActionDatas(HashMap<String, String> hm) {
        cp.putAll(hm);
    }

    public void storeData() {
        try {
            if (System.getProperty(OOZIE_JAVA_MAIN_CAPTURE_OUTPUT_FILE) != null) {
                FileOutputStream out = new FileOutputStream(System.getProperty(OOZIE_JAVA_MAIN_CAPTURE_OUTPUT_FILE));
                cp.store(out, "Store Oozie Action Output Properties");
                out.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
