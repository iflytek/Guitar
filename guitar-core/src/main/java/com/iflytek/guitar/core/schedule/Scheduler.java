package com.iflytek.guitar.core.schedule;

import com.iflytek.guitar.core.alg.AlgDef;
import com.iflytek.guitar.core.alg.RunAlgs;
import com.iflytek.guitar.core.output.OutputServer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 调度模块将形成一个DAG图，其中可以表示优先级、fork、join
 * 优先级，包括：
 * １．弱优先级，即按顺序启动，实际是否按顺序执行不关注
 * ２．强优先级，即前一个任务跑完才跑下一个任务（比如非常重要的任务或者有依赖关系的任务）
 * 强优先级位于DAG图的不同节点，弱优先级位于DAG图的同一个节点
 * <p>
 * 入库模块使用单独服务进行，不算入调度模块
 */
public class Scheduler {
    //正在运行DAG节点列表
    public List<DAGUnit> runningDagItems = new ArrayList<DAGUnit>();
    //第一个出现的异常
    public Exception firstException = null;

    private Configuration conf;

    public Scheduler(RunAlgs runAlgs, Configuration conf) throws IOException {
        this.conf = conf;

        List<RunAlgs> runAlgss = new ArrayList<RunAlgs>();
        runAlgss.add(runAlgs);
        initDag(runAlgss);
    }

    /**
     * 根据输入的报表，生成对应的执行DAG图，并将优先级最高的无依赖节点加入执行队列，供执行器获取
     *
     * @param runAlgss ： 输入执行报表列表
     *                 拆分和合并规则：
     *                 1. 报表如果依赖子频度报表，则拆分DAG不同节点
     *                 2. 不同报表数据源不同，则根据强优先级放在同一DAGUnit或者不同DAGUnit
     *                 3. 同一数据源，相同强优先级合并一次执行，优先级以最大的为准
     */
    private void initDag(List<RunAlgs> runAlgss) throws IOException {
        DAGUnit dagStart = DAGUnit.getStartNode();
        for (RunAlgs runalgs : runAlgss) {
            for (AlgDef algdef : runalgs.algs.values()) {
                dagStart.treeMerge(algdef, runalgs.startDate, runalgs.freq, runalgs.forceUpdate, runalgs.conf);
            }
        }

        dagStart.print(0);

        runningDagItems.add(dagStart);
    }

    public void run() throws Exception {
        RunAlgs runAlgsNext = null;
        while (checkStatus(conf)) {
            runAlgsNext = getNextRunAlgs();

            if (null != runAlgsNext) {
                runAlgsNext.start();
                System.out.println("now running Alg " + runAlgsNext.getUniqueCode());
            }

            try {
                Thread.sleep(2 * 1000L);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        if (null != firstException) {
            throw firstException;
        }
    }

    /*
     * 检查调度器是否应该结束运行，应该结束运行false，继续运行true
     */
    private boolean checkStatus(Configuration conf) {
        // 正在运行DAG节点列表为空，结束运行
        if (runningDagItems.isEmpty()) {
            return false;
        }

        List<DAGUnit> runningDagItemsTmp = new ArrayList<DAGUnit>();

        // 遍历正在运行DAG节点列表
        Iterator<DAGUnit> iter = runningDagItems.iterator();
        while (iter.hasNext()) {
            DAGUnit dagUnit = iter.next();

            // 标示该DAG节点是否全部完成
            boolean isAllFinshed = true;

            // 标示该DAG节点是否有失败任务
            boolean hasErr = false;

            // 遍历该DAG节点的所有任务
            for (RunAlgs runItem : dagUnit.runItems) {
                // 存在一个未完成的任务，则该DAG节点未完成，结束遍历
                if (!runItem.getIsFinished()) {
                    isAllFinshed = false;
                    break;
                }
                // 当前节点都已经完成，查看任务是否失败
                if (null != runItem.getErrInfo()) { // 存在失败任务，则记录之
                    hasErr = true;
                    if (null == dagUnit.errInfo) {
                        dagUnit.errInfo = runItem.getUniqueCode() + ":" + runItem.getErrInfo();
                    }
                    if (null == firstException) {
                        firstException = new Exception(dagUnit.errInfo);
                    }
                } else if (!runItem.isAddToOutputServer) { // 不存在失败任务，则添加到入库列表中，并标记该任务已加入入库列表
                    try {
                        OutputServer.get(conf).put(runItem);
                        runItem.isAddToOutputServer = true;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        if (null == dagUnit.errInfo) {
                            dagUnit.errInfo = runItem.getUniqueCode() + ":" + ex.getMessage();
                        }
                        if (null == firstException) {
                            firstException = new Exception(dagUnit.errInfo);
                        }
                    }
                }
            }

            dagUnit.isFinished = isAllFinshed;

            /*
             * 全部执行完成，则从正在运行DAG节点列表中删除该节点
             * 删除后，遍历该节点的所有子节点，如果其子节点的输入（通常是该节点的输出）都已经正确执行完成，则将其子节点加入正在运行DAG节点列表中
             * 如果该节点有错误，则其子节点全部不能被执行
             * 如果该节点正确，则遍历子节点，如果子节点的父节点没有全部成功执行完成，等待最后一个父节点执行完成时加入正在运行DAG节点列表中
             * 根据以上策略，如果有个节点执行错误，那么它的子节点永远不会加入到正在运行DAG节点列表中，直到正在运行DAG节点列表中为空退出
             *
             * 父子节点的依赖关系，1是数据依赖，2是优先级依赖，因此子节点可能有多个父节点
             * 父节点是最小频度任务(如Hourly)，子节点是高于最小频度的任务(如Daily)或优先级低于父节点的任务
             */
            if (isAllFinshed) {
                iter.remove();
                if (!hasErr && null != dagUnit.out) {
                    for (DAGUnit dagUnitOut : dagUnit.out) {
                        if (dagUnitOut.isAllInSuccess()) {
                            runningDagItemsTmp.add(dagUnitOut);
                        }
                    }
                }
            }
        }

        if (runningDagItemsTmp.size() > 0) {
            runningDagItems.addAll(runningDagItemsTmp);
        }

        return !runningDagItems.isEmpty();
    }

    private RunAlgs getNextRunAlgs() throws IOException {
        AlgDef.Priority current_max_priority_level = new AlgDef.Priority(-1, -1);
        RunAlgs priority_level_max_runalgs = null;

        for (DAGUnit dagUnit : runningDagItems) {
            for (RunAlgs runItem : dagUnit.runItems) {
                if (runItem.getPri().bigger(current_max_priority_level)
                        && !runItem.getIsStarted()) {
                    current_max_priority_level = runItem.getPri();
                    priority_level_max_runalgs = runItem;
                }
            }
        }

        return priority_level_max_runalgs;
    }

}
