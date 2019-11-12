package admin.core.scheduler;

import admin.constant.AdminConstant;
import admin.core.TesseractJobServiceDelegator;
import admin.core.scheduler.bean.CurrentTaskInfo;
import admin.core.scheduler.bean.TaskContextInfo;
import admin.core.scheduler.router.impl.HashRouter;
import admin.entity.TesseractExecutorDetail;
import admin.entity.TesseractFiredJob;
import admin.entity.TesseractLog;
import admin.entity.TesseractTrigger;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import tesseract.core.dto.TesseractExecutorRequest;
import tesseract.core.dto.TesseractExecutorResponse;
import tesseract.exception.TesseractException;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static admin.constant.AdminConstant.*;
import static admin.core.TesseractJobServiceDelegator.*;
import static tesseract.core.constant.CommonConstant.EXECUTE_MAPPING;
import static tesseract.core.constant.CommonConstant.HTTP_PREFIX;

/**
 * @author: nickle, liangxuekai
 * @description: 任务执行器代理，真正执行任务
 * @date: 2019-07-24 16:01
 */
@Slf4j
@Data
public class TaskExecutorDelegate {

    /**
     * 路由执行器
     *
     * @param taskContextInfo 任务上下文
     */
    public static void routerExecute(TaskContextInfo taskContextInfo) {
        TesseractTrigger trigger = taskContextInfo.getTrigger();
        //路由选择
        @NotNull Integer strategy = trigger.getStrategy();
        if (SCHEDULER_STRATEGY_BROADCAST.equals(strategy)) {
            executeBroadcast(taskContextInfo);
        } else if (SCHEDULER_STRATEGY_SHARDING.equals(strategy)) {
            executeSharding(taskContextInfo);
        } else {
            //正常调用
            executeGeneral(taskContextInfo);
        }
    }

    /**
     * 正常调用
     *
     * @param taskContextInfo
     */
    private static void executeGeneral(TaskContextInfo taskContextInfo) {
        TesseractTrigger trigger = taskContextInfo.getTrigger();
        List<TesseractExecutorDetail> executorDetailList = taskContextInfo.getExecutorDetailList();
        CurrentTaskInfo currentTaskInfo = new CurrentTaskInfo(taskContextInfo);
        TesseractExecutorDetail executorDetail = SCHEDULE_ROUTER_MAP.getOrDefault(trigger.getStrategy()
                , new HashRouter()).routerExecutor(executorDetailList);
        currentTaskInfo.setCurrentExecutorDetail(executorDetail);
        currentTaskInfo.setShardingIndex(-1);
        buildRequestAndSend(currentTaskInfo);
    }

    /**
     * 分片逻辑
     *
     * @param taskContextInfo
     */
    private static void executeSharding(TaskContextInfo taskContextInfo) {
        TesseractTrigger trigger = taskContextInfo.getTrigger();
        List<TesseractExecutorDetail> executorDetailList = taskContextInfo.getExecutorDetailList();
        @NotNull Integer shardingNum = trigger.getShardingNum();
        if (shardingNum == 0) {
            throw new TesseractException("分片数不能等于0");
        }
        int size = executorDetailList.size();
        int count = 0;
        for (int i = 0; i < shardingNum; i++) {
            CurrentTaskInfo currentTaskInfo = new CurrentTaskInfo(taskContextInfo);
            currentTaskInfo.setShardingIndex(count);
            //轮询发送给执行器执行
            if (count >= size) {
                count = 0;
            }
            currentTaskInfo.setCurrentExecutorDetail(executorDetailList.get(count));
            count++;
            buildRequestAndSend(currentTaskInfo);
        }
    }

    /**
     * 执行广播，并行发送任务到所有机器执行
     *
     * @param taskContextInfo
     */
    private static void executeBroadcast(TaskContextInfo taskContextInfo) {
        taskContextInfo.getExecutorDetailList().parallelStream().forEach(executorDetail -> {
            CurrentTaskInfo currentTaskInfo = new CurrentTaskInfo(taskContextInfo);
            currentTaskInfo.setCurrentExecutorDetail(executorDetail);
            currentTaskInfo.setShardingIndex(-1);
            buildRequestAndSend(currentTaskInfo);
        });
    }

    /**
     * 构建请求并发送
     * note:每个任务一个日志，重试的任务也是独立的日志，只不过重试的任务fireTrigger不变
     *
     * @param currentTaskInfo
     */
    private static void buildRequestAndSend(CurrentTaskInfo currentTaskInfo) {
        TesseractExecutorDetail currentExecutorDetail = currentTaskInfo.getCurrentExecutorDetail();
        TesseractLog tesseractLog = TesseractBeanFactory.createDefaultLog(currentTaskInfo);
        tesseractLog.setSocket(currentExecutorDetail.getSocket());
        tesseractLog.setMsg("执行中");
        //设置结束时间为0 表示未结束
        tesseractLog.setEndTime(0L);
        tesseractLog.setStatus(AdminConstant.LOG_WAIT);
        tesseractLog.setExecutorDetailId(currentExecutorDetail.getId());
        if (currentTaskInfo.isRetry()) {
            TesseractFiredJob firedJob = currentTaskInfo.getFiredJob();
            tesseractLog.setRetryCount(firedJob.getRetryCount());
        }
        logService.save(tesseractLog);
        currentTaskInfo.setLog(tesseractLog);
        //如果不是重试任务，设置firedTrigger.重试任务 已经拥有fire job
        if (!currentTaskInfo.isRetry()) {
            TesseractFiredJob firedJob = TesseractBeanFactory.createFiredJob(currentTaskInfo);
            firedJobService.save(firedJob);
            currentTaskInfo.setFiredJob(firedJob);
        } else {
            //如果是重试任务，重绑定fire job的 log id，此时log为新生成
            TesseractFiredJob firedJob = currentTaskInfo.getFiredJob();
            firedJob.setLogId(tesseractLog.getId());
            firedJobService.updateById(firedJob);
        }
        //构建请求发送
        TesseractExecutorRequest tesseractExecutorRequest = TesseractBeanFactory.createRequest(currentTaskInfo);
        currentTaskInfo.setExecutorRequest(tesseractExecutorRequest);
        doRequest(currentTaskInfo);
    }


    /**
     * 发送调度请求
     *
     * @param currentTaskInfo
     */
    private static void doRequest(CurrentTaskInfo currentTaskInfo) {
        TesseractExecutorRequest executorRequest = currentTaskInfo.getExecutorRequest();
        executorRequest.setParams(currentTaskInfo.getTaskContextInfo().getTrigger().getParams());
        TesseractExecutorDetail currentExecutorDetail = currentTaskInfo.getCurrentExecutorDetail();
        TesseractLog tesseractLog = currentTaskInfo.getLog();
        TesseractExecutorResponse response;
        String socket = currentExecutorDetail.getSocket();
        try {
            TaskExecutorDelegate.log.info("开始调度:{}", executorRequest);
            response = TesseractJobServiceDelegator.taskService.sendToExecutor(new URI(HTTP_PREFIX + socket + EXECUTE_MAPPING), executorRequest);
        } catch (TesseractException e) {
            TaskExecutorDelegate.log.error("发起调度异常", e);
            TesseractJobServiceDelegator.taskService.errorHandle(socket);
            response = TesseractExecutorResponse.builder().msg(e.getMsg()).status(TesseractExecutorResponse.FAIL_STATUS).build();
        } catch (Exception e) {
            TaskExecutorDelegate.log.error("发起调度异常", e);
            response = TesseractExecutorResponse.builder().msg(e.getMessage()).status(TesseractExecutorResponse.FAIL_STATUS).build();
        }
        //发送任务成功直接返回等待执行后更新日志状态
        if (response.getStatus() == TesseractExecutorResponse.SUCCESS_STATUS) {
            return;
        }
        //执行失败逻辑，执行到这一步必定是网络问题如，netty client失败，uri异常等
        tesseractLog.setStatus(LOG_FAIL);
        tesseractLog.setEndTime(System.currentTimeMillis());
        String msg = response.getMsg();
        if (msg != null) {
            tesseractLog.setMsg(msg);
        }
        /**
         * 循环执行，直到所有机器不可用，非任务执行异常
         */
        retry(currentTaskInfo, false);
    }

    /**
     * 失败重试：
     * 1、移除当前失败执行器，小于重试次数，更新日志为失败且发送邮件
     * 2、大于重试次数，更新日志为失败且发送邮件，移除fire job
     *
     * @param currentTaskInfo
     */
    public static void retry(CurrentTaskInfo currentTaskInfo, boolean incrRetryCount) {
        List<TesseractExecutorDetail> executorDetailList = currentTaskInfo.getTaskContextInfo().getExecutorDetailList();
        if (executorDetailList.size() > 1) {
            doRetry(currentTaskInfo, incrRetryCount);
        } else {
            doFailWithFireJob(currentTaskInfo);
        }
    }

    /**
     * 开始重试,重试步骤:
     * 1、记录当前日志并发送失败邮件
     * 2、从当前列表中移除当前列表执行器
     * 3、更新正在执行的trigger的重试次数
     * 4、根据调度策略判断是否启动重试并选择执行机器
     */
    private static void doRetry(CurrentTaskInfo currentTaskInfo, boolean incrRetryCount) {
        log.info("进入重试逻辑");
        TesseractLog tesseractLog = currentTaskInfo.getLog();
        List<TesseractExecutorDetail> executorDetailList = currentTaskInfo.getTaskContextInfo().getExecutorDetailList();
        TesseractFiredJob firedJob = currentTaskInfo.getFiredJob();
        TesseractTrigger trigger = currentTaskInfo.getTaskContextInfo().getTrigger();
        //广播策略不重试
        if (trigger.getStrategy().equals(SCHEDULER_STRATEGY_BROADCAST)) {
            doFailWithoutFireJob(currentTaskInfo);
            return;
        }
        //移除失败机器
        removeExecutorDetail(executorDetailList, currentTaskInfo.getCurrentExecutorDetail());
        //更新正在执行触发器的重试次数
        if (incrRetryCount) {
            firedJob.setRetryCount(firedJob.getRetryCount() + 1);
            tesseractLog.setRetryCount(firedJob.getRetryCount() + 1);
            firedJobService.updateById(firedJob);
        }
        doFailWithoutFireJob(currentTaskInfo);
        if (trigger.getStrategy().equals(SCHEDULER_STRATEGY_SHARDING)) {
            //分片仅重试当前分片索引,通过hash策略随机选择一台机器重试
            TesseractExecutorDetail tesseractExecutorDetail = SCHEDULE_ROUTER_MAP.get(SCHEDULER_STRATEGY_HASH).routerExecutor(executorDetailList);
            currentTaskInfo.setCurrentExecutorDetail(tesseractExecutorDetail);
        } else {
            TesseractExecutorDetail executorDetail = SCHEDULE_ROUTER_MAP.getOrDefault(trigger.getStrategy()
                    , new HashRouter()).routerExecutor(executorDetailList);
            currentTaskInfo.setCurrentExecutorDetail(executorDetail);
        }
        currentTaskInfo.setRetry(true);
        currentTaskInfo.setLog(null);
        currentTaskInfo.setExecutorRequest(null);
        buildRequestAndSend(currentTaskInfo);
    }

    /**
     * 移除指定的执行器
     */
    private static void removeExecutorDetail(List<TesseractExecutorDetail> executorDetailList, TesseractExecutorDetail needRemoveDetail) {
        Iterator<TesseractExecutorDetail> iterator = executorDetailList.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getId().equals(needRemoveDetail.getId())) {
                iterator.remove();
                log.info("重试移除机器:{}", needRemoveDetail);
                return;
            }
        }
    }

    /**
     * 保存失败日志并产生报警邮件
     * 1、更改日志状态
     * 2、发送邮件
     *
     * @param msg             报错信息
     * @param taskContextInfo 触发器任务上下文
     */
    public static void doFail(String msg, TaskContextInfo taskContextInfo) {
        CurrentTaskInfo currentTaskInfo = new CurrentTaskInfo(taskContextInfo);
        TesseractLog tesseractLog = TesseractBeanFactory.createDefaultLog(currentTaskInfo);
        tesseractLog.setMsg(msg);
        tesseractLog.setStatus(LOG_FAIL);
        logService.save(tesseractLog);
        mailSender.logSendMail(tesseractLog);
    }

    /**
     * 保存失败日志并产生报警邮件
     * 1、保存日志状态
     * 2、发送邮件
     *
     * @param currentTaskInfo 当前执行的任务上下文
     */
    private static void doFailWithoutFireJob(CurrentTaskInfo currentTaskInfo) {
        log.info("执行任务失败:{},开始记录日志并发送邮件", currentTaskInfo);
        TesseractLog tesseractLog = currentTaskInfo.getLog();
        logService.updateById(tesseractLog);
        mailSender.logSendMail(tesseractLog);
    }

    /**
     * 更新失败日志并产生报警邮件
     * 1、更改日志状态
     * 2、发送邮件
     * 3、更改firedJob
     *
     * @param currentTaskInfo
     */
    public static void doFailWithFireJob(CurrentTaskInfo currentTaskInfo) {
        TesseractFiredJob firedJob = currentTaskInfo.getFiredJob();
        firedJobService.removeById(firedJob);
        doFailWithoutFireJob(currentTaskInfo);
    }
}
