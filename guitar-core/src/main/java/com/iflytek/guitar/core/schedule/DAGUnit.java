package com.iflytek.guitar.core.schedule;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.alg.Input;
import com.iflytek.guitar.core.alg.RunAlgs;
import com.iflytek.guitar.share.utils.Constants;
import com.iflytek.guitar.share.utils.SafeDate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

public class DAGUnit {
    private static Log LOG = LogFactory.getLog(DAGUnit.class);

    public List<DAGUnit> in = null;
    public List<DAGUnit> out = null;

    public List<RunAlgs> runItems = new ArrayList<RunAlgs>();

    // finished when all runItems finished success.
    public boolean isFinished = false;
    public String errInfo = null;

    /**
     * this DAGUnit must be root node!
     *
     * @param algdef
     */
    public void treeMerge(AlgDef algdef, Date startDate, AlgDef.Frequence freq, boolean forceUpdate,
                          Configuration conf) throws IOException {
        // if not force update the report output data which is already exisits, not run the report alg and not merge
        if (!forceUpdate) {
            FileSystem fs = FileSystem.get(conf);
            Path algOutput = new Path(algdef.getOutPath(freq, startDate, conf));
            if (fs.exists(algOutput)) {
                LOG.info(algdef.alg_name + " of Freq " + freq + " at date "
                        + SafeDate.Date2Format(startDate, Constants.DATE_FORMAT_BASIC)
                        + " has been finished,"
                        + " the output: " + algOutput + " is exist,"
                        + " will skip!!!");
                return;
            }
        }

        // if bigger frequency and check type is all, ensure that needed report output data generated
        if (freq.bigger(algdef.minFreq) && algdef.input.check_type.equals(Input.CheckType.all)) {
            Date dt_end = freq.getEndDate(startDate);
            Calendar cal_beg = Calendar.getInstance();
            Calendar cal_end = Calendar.getInstance();
            cal_beg.setTime(startDate);
            cal_end.setTime(dt_end);

            AlgDef.Frequence freq_child = freq.getChildFreq();
            while (cal_beg.before(cal_end)) {
                treeMerge(algdef, cal_beg.getTime(), freq_child, false, conf);
                AlgDef.Frequence.add(cal_beg, freq_child, 1);
            }
        }

        // if smallest frequency alg or needed report output data generated, add the alg to DAG
        insertAlgToDag(this, algdef, startDate, freq, forceUpdate, conf);
    }

    /**
     * 中间数据已生成或者最小频度报表，将报表添加到DAG中
     * <p>
     * 遍历算法：
     * 1. 如果比当前节点强优先级大，报错（从优先级最大的start节点开始遍历，此情况不存在）
     * 2. 如果和当前节点强优先级相同，且频度相同，根据数据源查看是否能够合并
     * 3. 如果和当前节点强优先级相同，且频度不相同，如果不包含相同报表，则合并，否则跳到5
     * 4. 如果和当前节点强优先级小，则跳到逻辑5
     * 5. 遍历子节点
     * <p>
     * But，当前场景下，输入数据的报表频度是相同的，不存在复杂的依赖关系，所以当前的DAG也是一个list结构，可简化算法
     */
    private void insertAlgToDag(DAGUnit dagu,   // root of DAG
                                AlgDef algdef,  // alg to insert
                                Date startDate,
                                AlgDef.Frequence freq,
                                boolean forceUpdate,
                                Configuration conf) throws IOException {

        // 查看该dag节点是否有该报表的子报表
        boolean isParentAlg = false;
        for (RunAlgs ra : dagu.runItems) {
            if (freq.bigger(ra.freq)) {
                for (String ad : ra.algs.keySet()) {
                    if (ad.equals(algdef.alg_name)) {
                        isParentAlg = true;
                        break;
                    }
                }
            }
        }

        if (algdef.priority.s > dagu.getSPriority()) {
            throw new IOException("alg " + algdef.alg_name + "'s priority is bigger than current DAGUnit!("
                    + algdef.priority.s + ">" + dagu.getSPriority() + ")");
        } else if (algdef.priority.s == dagu.getSPriority() && !isParentAlg) {
            // 直接将该算法与该节点合并
            boolean isMerged = false;
            for (RunAlgs runalg : dagu.runItems) {
                /*
                 * 频度、起始时间、输入路径三者相同，认为两个报表可以合并运行
                 */
                if (runalg.freq.equals(freq) && runalg.startDate.equals(startDate)) {
                    List<String> inputs1 = runalg.getInputs();
                    List<String> inputs2 = algdef.getInputPath(freq, startDate, conf);
                    Collections.sort(inputs1);
                    Collections.sort(inputs2);
                    if (inputs1.size() == inputs2.size() && inputs1.size() != 0) {
                        boolean same = true;
                        for (int i = 0; i < inputs1.size(); i++) {
                            if (!inputs1.get(i).equals(inputs2.get(i))) {
                                same = false;
                                break;
                            }
                        }
                        if (same) {
                            runalg.algs.put(algdef.alg_name, algdef);
                            isMerged = true;
                            break;
                        }
                    }

                }
            }
            // 不能合运行，则并发运行
            if (!isMerged) {
                RunAlgs runalgs = new RunAlgs(algdef, startDate, freq, forceUpdate, conf);
                dagu.addRunItemPriority(runalgs);
            }
            // 查找该节点的子节点
        } else {
            // 无孩子节点
            if (dagu.out == null || dagu.out.size() == 0) {
                RunAlgs runalgs = new RunAlgs(algdef, startDate, freq, forceUpdate, conf);
                DAGUnit unit = new DAGUnit();
                unit.runItems.add(runalgs);
                unit.addIn(dagu);
                dagu.addOut(unit);
                // 孩子节点优先级小·
            } else if (dagu.out.iterator().next().getSPriority() < algdef.priority.s) {
                RunAlgs runalgs = new RunAlgs(algdef, startDate, freq, forceUpdate, conf);
                DAGUnit unit = new DAGUnit();
                unit.runItems.add(runalgs);
                unit.addIn(dagu);
                unit.addOut(dagu.out.iterator().next());
                dagu.out.set(0, unit);
                unit.out.iterator().next().in.set(0, unit);
            }
            // 孩子节点优先级高
            else {
                insertAlgToDag(dagu.out.iterator().next(), algdef, startDate, freq, forceUpdate, conf);
            }
        }

    }

    public static DAGUnit getStartNode() {
        DAGUnit start = new DAGUnit();
        start.isFinished = true;
        return start;
    }

    public void addIn(DAGUnit u) {
        if (null == in) {
            in = new ArrayList<DAGUnit>();
        }
        in.add(u);
    }

    public void addOut(DAGUnit u) {
        if (null == out) {
            out = new ArrayList<DAGUnit>();
        }
        out.add(u);
    }

    public void rmIn(DAGUnit u) {
        if (null != in) {
            for (int i = in.size() - 1; i >= 0; i--) {
                if (in.get(i) == u) {
                    in.remove(i);
                    break;
                }
            }
        }
    }

    public void rmOut(DAGUnit u) {
        if (null != out) {
            for (int i = out.size() - 1; i >= 0; i--) {
                if (out.get(i) == u) {
                    out.remove(i);
                    break;
                }
            }
        }
    }

    public boolean hasUnitInIn(DAGUnit u) {
        for (DAGUnit unit : in) {
            if (unit == u) {
                return true;
            }
        }
        return false;
    }

    public boolean hasUnitInOut(DAGUnit u) {
        for (DAGUnit unit : out) {
            if (unit == u) {
                return true;
            }
        }
        return false;
    }

    public boolean checkLink() {
        if (null != in) {
            for (DAGUnit u : in) {
                if (!u.hasUnitInOut(this)) {
                    return false;
                }
            }
        }

        if (null != out) {
            for (DAGUnit u : out) {
                if (!u.checkLink()) {
                    return false;
                }
            }
        }

        return true;
    }

    public int getSPriority() {
        if (runItems.size() == 0) {
            // start node
            return Integer.MAX_VALUE;
        }

        return runItems.iterator().next().algs.values().iterator().next().priority.s;
    }

    public void addRunItemPriority(RunAlgs runalgs) throws IOException {
        int i = runItems.size();
        for (; i > 0 && runItems.get(i - 1).getPri().w < runalgs.getPri().w; i--) {
        }

        runItems.add(i, runalgs);
    }

    public boolean isAllInSuccess() {
        for (DAGUnit dagu : in) {
            if (!dagu.isFinished || errInfo != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param l：缩进
     */
    public void print(int l) {
        printTable(l, "A new DAGUnit");
        for (RunAlgs algs : runItems) {
            printTable(l, algs.getUniqueCode());
        }

        if (null != out) {
            for (DAGUnit unit : out) {
                unit.print(l + 1);
            }
        }
    }

    private void printTable(int l, String s) {
        for (int i = 0; i < l; i++) {
            System.out.print("|--");
        }
        System.out.println(s);
    }

}
