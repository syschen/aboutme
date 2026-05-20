# 工作流运行机制 - 关键代码流程分析

## 文档概述

本文档深入分析关键代码片段，详细说明每个步骤的执行流程、条件判断和状态转移。

---

## 第一部分：消息消费入口 (WorkFlowConsumerService)

### 1.1 receiveWorkFlowData 流程分析

**核心职责**:
- Mafka消息接收和解析
- 基础验证和错误处理
- 调用核心处理服务

**代码流程**:

```java
public boolean receiveWorkFlowData(String workFlowData) {
    // Step 1: 空值检查
    if (StringUtils.isBlank(workFlowData)) {
        log.warn("接收到空的工作流消息");
        Cat.logEvent("WorkFlowConsumerErr", "emptyMessage");
        return true;  // ✓ 返回true表示ACK，不重投
    }

    // Step 2: 解析消息
    WorkFlowMessage workFlowMessage = null;
    try {
        workFlowMessage = Obj.tryToParseJsonString(workFlowData, WorkFlowMessage.class).orNull();
        if (workFlowMessage == null || workFlowMessage.getInstanceId() == null) {
            log.error("工作流消息解析失败或实例ID为空: {}", workFlowData);
            Cat.logEvent("WorkFlowConsumerErr", "parseMessageFailed");
            return true;  // ✓ ACK失败消息，避免死循环
        }

        // Step 3: 黑名单检查
        List<String> excludeBizIdentities = lionConfig.getExcludeProcessBizIdentities();
        if (CollectionUtils.isNotEmpty(excludeBizIdentities)
            && excludeBizIdentities.contains(workFlowMessage.getBizIdentity())) {
            log.info("工作流消息业务标识在黑名单中，跳过处理");
            return true;  // ✓ ACK黑名单消息
        }

        // Step 4: 调用核心处理
        processWorkFlowMessage(workFlowMessage);
    } catch (Exception e) {
        String instanceId = workFlowMessage != null ?
            String.valueOf(workFlowMessage.getInstanceId()) : "unknown";
        log.error("MIS处理工作流消息异常: instanceId={}", instanceId, e);
        Cat.logEvent("WorkFlowConsumerErr", "processError");
    }
    return true;  // ✓ 统一返回true（错误已记录）
}
```

**关键决策点**:
1. **为什么空消息返回true?** - 避免消息重投导致的死循环
2. **为什么解析错误ACK?** - 坏数据无法恢复，ACK避免消费阻塞
3. **黑名单作用** - 某些业务身份可能在当前消费端不需要处理

### 1.2 processWorkFlowMessage 流程分析

**核心职责**:
- 从数据库查询实例最新状态
- 状态预检查
- 调用WorkFlowService进行实际处理

**代码流程**:

```java
private void processWorkFlowMessage(WorkFlowMessage workFlowMessage) {
    Long instanceId = workFlowMessage.getInstanceId();

    // Step 1: 创建CAT事务（用于性能监控）
    Transaction transaction = Cat.newTransaction("WorkFlowConsumer", "process");
    try {
        // Step 2: 从数据库查询最新实例
        CustomWorkFlowInstanceEntity entity = workFlowModelService.selectById(instanceId);
        if (entity == null) {
            log.warn("工作流实例不存在: instanceId={}", instanceId);
            transaction.setStatus("instanceNotFound");
            Cat.logEvent("WorkFlowConsumerErr", "instanceNotFound");
            return;  // ✗ 实例不存在，返回
        }

        // Step 3: 检查实例状态 - 避免处理已完成的实例
        // 终态: FINISHED, FAILURE, DISUSE
        if (WorkFlowInstanceStatusEnum.isFinalStatus(entity.getStatus())) {
            log.info("工作流实例已处于终态，跳过处理: instanceId={}, status={}",
                instanceId, entity.getStatus());
            transaction.setSuccessStatus();
            return;  // ✗ 已是终态，返回
        }

        // Step 4: 调用核心处理器进行同步处理
        // 这里会进入 WorkFlowService.process()
        workFlowService.process(entity);
        transaction.setSuccessStatus();
        Cat.logEvent("WorkFlowConsumer", "processSuccess");

    } catch (Exception e) {
        log.error("MIS处理工作流实例异常: instanceId={}", instanceId, e);
        Cat.logEvent("WorkFlowConsumerErr", "processInstanceError");
        transaction.setStatus(e);
        throw e;  // ✗ 异常会导致消息重投
    } finally {
        transaction.complete();
    }
}
```

**关键设计**:
- **最新状态查询**: 避免消息中的状态过期，每次处理都查询最新状态
- **终态检查**: 性能优化，已完成的实例无需再处理
- **同步处理**: 这是一个同步调用，处理完成才返回

---

## 第二部分：核心处理服务 (WorkFlowService)

### 2.1 process() 方法的完整流程

**核心职责**:
- 状态检查
- 环境检查
- 分布式锁竞争
- 业务逻辑编排

**代码流程详解**:

```java
public void process(CustomWorkFlowInstanceEntity entity) {
    // =========================== 第一层：快速检查 ===========================

    // 检查 1: 实例是否已是终态
    if (WorkFlowInstanceStatusEnum.isFinalStatus(entity.getStatus())) {
        return;  // 快速返回，无需处理
    }

    // 检查 2: 执行环境是否匹配
    // 比如: 实例在测试环境创建，但在生产环境消费 → 跳过
    if (WorkFlowModelHelper.isNotExecuteEnv(entity)) {
        return;  // 环境不匹配，返回
    }

    // =========================== 第二层：分布式锁 ===========================

    // 从Lion配置获取锁超时时间（可根据工作流模板名称调整）
    Integer lockTimeoutMinutes = lionConfig.getCustomWorkflowLockTimeoutMinutes()
        .get(entity.getTemplateName());
    if (lockTimeoutMinutes == null) {
        lockTimeoutMinutes = 10;  // 默认10分钟
    }
    int lockExpireSeconds = lockTimeoutMinutes * 60;

    // 生成锁名称：工作流类型 + 模板名称 + 实例ID
    String lockName = "workFlow-" + entity.getTemplateName() + "-" + entity.getId();

    // =========================== 第三层：自旋循环处理 ===========================

    for (;;) {  // 无限循环，直到处理完成或返回
        // Step 1: 尝试获取分布式锁
        boolean getLock = redisLockGateway.tryLock(
            DistributedLockSceneEnum.CUSTOM_WORK_FLOW.getKey(),
            lockName,
            lockExpireSeconds
        );

        // 获取失败，其他消费者正在处理，直接返回
        if (!getLock) {
            return;  // ✗ 获取锁失败，本次处理结束
        }

        // ======================== 获得锁后的处理 ========================

        // Step 2: 设置MDC日志信息（用于链路追踪）
        MDC.put("workFlowId", String.valueOf(entity.getId()));
        MDC.put("relatedId", entity.getRelatedId());
        MDC.put("bizIdentity", entity.getBizIdentity());

        try {
            // Step 3: 重新查询最新实例（获得锁后需要重新查询，防止过期数据）
            CustomWorkFlowInstanceEntity newEntity = workFlowModelService.selectById(entity.getId());
            if (newEntity == null) {
                LOGGER.warn("查库取实体为空,id:{},内容:{}", entity.getId(), entity);
                return;  // 实例被删除，返回
            }

            // Step 4: 检查状态，如果是初始化状态，更新为处理中
            if (Objects.equals(newEntity.getStatus(), WorkFlowInstanceStatusEnum.INIT.getStatus())) {
                newEntity.setStatus(WorkFlowInstanceStatusEnum.PROCESSING.getStatus());
                workFlowModelService.updateById(newEntity);  // 立即保存到数据库
                LOGGER.info("更新状态 INIT → PROCESSING, instanceId: {}", newEntity.getId());
            }

            // Step 5: 记录日志
            LOGGER.info("查库取实体id:{}, digestLog:{}, currentStepName:{}, " +
                "taskNodeSnapshot:{}, runtime:{}",
                entity.getId(),
                newEntity.getDigestLog(),
                newEntity.getCurrentStepName(),
                newEntity.getTaskNodeSnapshot(),
                newEntity.getRuntimeContext());

            try {
                // ==================== 核心业务逻辑 ====================

                // Step 6a: 构建工作流DAG实例（从JSON反序列化）
                WorkFlowDagInstance workFlowDagInstance =
                    WorkFlowModelHelper.buildWorkFlowDagDefine(newEntity);

                // Step 6b: 构建工作流任务实例（根据当前步骤名查找）
                WorkFlowTaskInstance workFlowTaskInstance =
                    WorkFlowModelHelper.buildWorkFlowTaskNode(newEntity, workFlowDagInstance);

                // Step 6c: 构建运行时上下文（从快照和上下文恢复）
                WorkFlowRuntimeContext runtimeContext =
                    WorkFlowModelHelper.buildWorkFlowRuntime(newEntity, workFlowTaskInstance);

                // Step 7: 检查是否需要停止（实例可能在处理过程中变为终态）
                if (needStop(workFlowDagInstance)) {
                    return;  // ✗ 终态，返回
                }

                // Step 8: 检查是否需要等待
                if (needWait(runtimeContext)) {
                    // 计算延迟秒数 = (指定时间 - 当前时间) / 1000
                    long delaySeconds = Math.max(1,
                        (runtimeContext.getTaskRuntimeContext().getAppointExecuteTimeMills()
                         - System.currentTimeMillis()) / 1000);

                    // 小于60秒直接发送延迟消息，避免延迟消息的成本
                    if (delaySeconds < 60) {
                        workFlowMessageSender.delaySendWorkFlowMessage(newEntity, delaySeconds);
                    }
                    return;  // ✗ 需要等待，返回
                }

                // Step 9: 推进工作流一步
                // 这是真正的业务处理发生的地方
                WorkFlowTaskNodeStatusEnum taskResultStatus =
                    workFlowProcessor.pushOneStep(workFlowTaskInstance, runtimeContext);

                // Step 10: 等待数据库从库同步
                // 原因：主从延迟可能导致下一个消费者读到旧数据
                Thread.sleep(2000);

                // Step 11: 检查返回状态，决定是否继续循环
                // LATER_CONTINUE_PROCESSING: 延迟继续处理，需要发送延迟消息
                // WAITING_NODE_RESULT: 等待某个结果（轮询、重试等），需要发送延迟消息
                if (taskResultStatus == WorkFlowTaskNodeStatusEnum.LATER_CONTINUE_PROCESSING
                    || taskResultStatus == WorkFlowTaskNodeStatusEnum.WAITING_NODE_RESULT) {
                    return;  // ✗ 返回，等待下一轮消息
                }
                // 其他状态继续循环（比如SUCCESS且有下一步）

            } catch (Throwable e) {
                // ==================== 异常处理 ====================
                LOGGER.error("process error", e);

                // 降级处理：强制设置为失败状态
                newEntity.setStatus(WorkFlowInstanceStatusEnum.FAILURE.getStatus());
                newEntity.setSubStatus(WorkFlowInstanceSubStatusEnum.FORCED_FAIL.getStatus());
                newEntity.setDigestLog(newEntity.getDigestLog() +
                    "process error:" + ExceptionUtils.getMessage(e));

                // 执行失败回调
                workFlowCallbackManager.executeErrorCallbacks(newEntity);

                // 保存到数据库
                workFlowModelService.updateById(newEntity);

                // 恢复中断状态（如果是InterruptedException）
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }

                throw new RuntimeException(e);  // ✗ 抛出异常，消息重投
            }

        } finally {
            // ==================== 清理资源 ====================
            MDC.remove("workFlowId");
            MDC.remove("relatedId");
            MDC.remove("bizIdentity");

            // 释放分布式锁
            redisLockGateway.unLock(
                DistributedLockSceneEnum.CUSTOM_WORK_FLOW.getKey(),
                lockName
            );
        }
    }  // 循环结束
}
```

**关键设计点**:

1. **多层检查的目的**:
   - 快速检查 (终态、环境): 0-1ms，避免不必要的锁竞争
   - 分布式锁: 防止多消费者并发处理
   - 重新查询: 获得锁后需要重新查询，防止过期数据

2. **自旋循环的作用**:
   - 同一消息中可能多次推进步骤（比如连续的快速步骤）
   - 避免频繁的消息投递和消费，提高吞吐量
   - 直到到达需要等待的步骤才返回

3. **Thread.sleep(2000) 的目的**:
   - 数据库主从同步延迟通常在毫秒级
   - 2秒等待确保从库已同步
   - 下一轮消息处理时能读到最新数据

4. **异常处理的策略**:
   - 捕获所有异常，设置FAILURE状态
   - 执行失败回调（可能包括告知业务系统失败）
   - 抛出异常导致消息重投（Mafka的自动重试机制）

### 2.2 needStop() 和 needWait() 方法

```java
// 检查是否需要停止
private boolean needStop(WorkFlowDagInstance instance) {
    // 检查DAG实例的状态是否是终态
    // 理由：在获得锁和现在之间，其他操作可能已经将实例转为终态
    return WorkFlowInstanceStatusEnum.isFinalStatus(instance.getStatus());
}

// 检查是否需要等待
private boolean needWait(WorkFlowRuntimeContext runtimeContext) {
    // 如果设置了指定执行时间且时间未到，需要等待
    return runtimeContext.getTaskRuntimeContext().getAppointExecuteTimeMills() != null &&
           runtimeContext.getTaskRuntimeContext().getAppointExecuteTimeMills()
           > System.currentTimeMillis();
}
```

---

## 第三部分：工作流推进 (WorkFlowProcessor)

### 3.1 pushOneStep() 方法

**核心职责**:
- 调用LocalTaskProcessor执行任务
- 根据结果处理状态转移
- 更新数据库

**代码流程**:

```java
@Transactional(value = "zebraTransactionManager0", rollbackFor = Exception.class)
public WorkFlowTaskNodeStatusEnum pushOneStep(WorkFlowTaskInstance workFlowTaskInstance,
                                              WorkFlowRuntimeContext runtimeContext) {
    // Step 1: 执行任务
    WorkFlowTaskNodeStatusEnum taskResultStatus =
        taskProcess(workFlowTaskInstance, runtimeContext);

    // Step 2: 检查当前步骤名是否为空（质量检查）
    CustomWorkFlowInstanceEntity entity =
        WorkFlowModelHelper.convertWorkFlowEntity(workFlowTaskInstance, runtimeContext);
    if (entity.getCurrentStepName() == null) {
        throw new IllegalStateException("当前任务stepName为空异常");
    }

    // Step 3: 更新数据库
    workFlowModelService.updateById(entity);

    // Step 4: 返回状态
    return taskResultStatus;
}

private WorkFlowTaskNodeStatusEnum taskProcess(WorkFlowTaskInstance workFlowTaskInstance,
                                               WorkFlowRuntimeContext runtimeContext) {
    // Step 1: 获取任务处理器（通常是LocalTaskProcessor）
    ITaskProcessor iTaskProcessor = TaskProcessorFactory.getTaskProcessor();

    // Step 2: 初始化开始执行时间
    if (runtimeContext.getTaskRuntimeContext() != null &&
        runtimeContext.getTaskRuntimeContext().getStartExecuteTimeMills() == null) {
        runtimeContext.getTaskRuntimeContext()
            .setStartExecuteTimeMills(System.currentTimeMillis());
    }

    // Step 3: 执行任务
    WorkFlowTaskNodeStatusEnum status =
        iTaskProcessor.action(workFlowTaskInstance, WorkFlowTaskActionEnum.RUN, runtimeContext);

    // Step 4: 根据返回状态处理
    switch (status) {
        case SUCCESS:
            // ============ 成功状态处理 ============
            onSuccessStatus(workFlowTaskInstance, runtimeContext);
            break;

        case FINISH_FAIL_DIAGNOSIS:
        case FAIL:
            // ============ 失败状态处理 ============
            onFailStatus(workFlowTaskInstance, runtimeContext);
            break;

        case PROCESSING:
        case NODE_RETRY_PROCESSING:
        case WAITING_NODE_RESULT:
        case WAITING_FAIL_DIAGNOSIS_RESULT:
        case CONTINUE_PROCESSING:
        case LATER_CONTINUE_PROCESSING:
        case QUEUE_PROCESSING:
            // ============ 中间状态（无需处理） ============
            break;

        case WORKFLOW_DISUSE:
            // ============ 工作流停用处理 ============
            onWorkFlowDisuseStatus(workFlowTaskInstance, runtimeContext);
            break;

        default:
            throw new IllegalStateException("未知任务状态");
    }

    return status;
}
```

### 3.2 onSuccessStatus() 方法详解

**作用**: 处理任务成功的情况，决定下一步操作

```java
private void onSuccessStatus(WorkFlowTaskInstance workFlowTaskInstance,
                            WorkFlowRuntimeContext runtimeContext) {
    // Step 1: 记录性能指标
    logExecuteTimeMetric(workFlowTaskInstance, runtimeContext, "success");

    // Step 2: 构建摘要日志
    String digestLog = String.format("%s:success", workFlowTaskInstance.getStepName());

    // Step 3: 清除当前任务的运行时上下文（因为任务已完成）
    runtimeContext.setTaskRuntimeContext(null);

    // Step 4: 追加摘要日志
    runtimeContext.getInstanceRuntimeContext().appendDigest(digestLog);

    // Step 5: 获取下一个步骤的名称
    String nextStepName = workFlowTaskInstance.getWorkFlowStepNodeDefine()
        .getNextStepOnSuccess();

    // Step 6: 检查是否有下一步
    if (StringUtils.isBlank(nextStepName)) {
        // ============ 没有下一步，工作流完成 ============

        // 设置工作流状态为完成
        workFlowTaskInstance.getWorkFlowDagInstance()
            .setStatus(WorkFlowInstanceStatusEnum.FINISHED.getStatus());

        // 执行成功回调（通知业务系统工作流完成）
        workFlowCallbackManager.executeCallbacks(
            WorkFlowCallbackTypeEnum.SUCCESS,
            workFlowTaskInstance,
            runtimeContext
        );
    } else {
        // ============ 有下一步，更新为下一个步骤 ============
        workFlowTaskInstance.setStepName(nextStepName);
    }

    // Step 7: 状态机转移（成功路径）
    // 用于跟踪工作流的细粒度执行进度
    WorkFlowInstanceSubStatusEnum nextSubStatus =
        WorkFlowInstanceSubStatusEnum.stateMachineMove(
            true,  // true表示成功路径
            WorkFlowInstanceSubStatusEnum.valueOf(
                workFlowTaskInstance.getWorkFlowDagInstance().getSubStatus()
            )
        );
    workFlowTaskInstance.getWorkFlowDagInstance()
        .setSubStatus(nextSubStatus.getStatus());
}
```

**关键设计**:
- **清除taskRuntimeContext**: 表示任务已完成，下一个步骤不再需要这个上下文
- **是否检查next**: 通过next为空判断工作流是否完成
- **状态机转移**: 记录工作流的执行进度，用于故障诊断和监控

### 3.3 onFailStatus() 方法详解

**作用**: 处理任务失败的情况，决定是否重试/重跑或终止

```java
private void onFailStatus(WorkFlowTaskInstance workFlowTaskInstance,
                         WorkFlowRuntimeContext runtimeContext) {
    // Step 1: 记录性能指标
    logExecuteTimeMetric(workFlowTaskInstance, runtimeContext, "fail");

    // Step 2: 构建摘要日志
    String digestLog = String.format("%s:fail", workFlowTaskInstance.getStepName());

    // Step 3: 清除当前任务的运行时上下文
    runtimeContext.setTaskRuntimeContext(null);

    // Step 4: 追加摘要日志
    runtimeContext.getInstanceRuntimeContext().appendDigest(digestLog);

    // Step 5: 设置工作流状态为失败
    workFlowTaskInstance.getWorkFlowDagInstance()
        .setStatus(WorkFlowInstanceStatusEnum.FAILURE.getStatus());

    // Step 6: 状态机转移（失败路径）
    WorkFlowInstanceSubStatusEnum nextSubStatus =
        WorkFlowInstanceSubStatusEnum.stateMachineMove(
            false,  // false表示失败路径
            WorkFlowInstanceSubStatusEnum.valueOf(
                workFlowTaskInstance.getWorkFlowDagInstance().getSubStatus()
            )
        );
    workFlowTaskInstance.getWorkFlowDagInstance()
        .setSubStatus(nextSubStatus.getStatus());

    // Step 7: 执行失败回调（通知业务系统工作流失败）
    workFlowCallbackManager.executeCallbacks(
        WorkFlowCallbackTypeEnum.FAILURE,
        workFlowTaskInstance,
        runtimeContext
    );
}
```

---

## 第四部分：本地任务执行 (LocalTaskProcessor)

### 4.1 taskRun() 流程分析

**核心职责**:
- 获取具体的任务运行器
- 检查工作流是否被停止
- 调用运行器执行任务
- 处理返回结果

**代码流程**:

```java
protected WorkFlowTaskNodeStatusEnum taskRun(WorkFlowTaskInstance workFlowTaskInstance,
                                             WorkFlowRuntimeContext runtimeContext) {
    // Step 1: 获取步骤名称
    String stepName = workFlowTaskInstance.getWorkFlowStepNodeDefine().getName();

    // Step 2: 通过工厂模式获取任务运行器
    ITaskRunner taskRunner =
        TaskRunnerFactory.getTaskRunner(
            workFlowTaskInstance.getWorkFlowStepNodeDefine().getRunnerName()
        );
    Preconditions.checkNotNull(taskRunner, "runner为空");

    // Step 3: 检查工作流是否被停止（可能由管理端手动停止）
    boolean isStoped = workFlowCallbackManager.isStoped(
        workFlowTaskInstance,
        runtimeContext
    );

    WorkFlowTaskResult workFlowTaskResult;

    // Step 4a: 如果被停止，执行取消操作
    if (isStoped) {
        taskRunner.cancel(workFlowTaskInstance, runtimeContext);  // 通知Runner取消
        runtimeContext.getInstanceRuntimeContext()
            .appendDigest(stepName + ":任务被停止");
        workFlowTaskResult = WorkFlowTaskResult.buildTaskStop();  // 返回停止状态

        LOGGER.info("工作流任务已被停止, instanceId:{}, bizIdentity:{}, relatedId:{}",
            workFlowTaskInstance.getWorkFlowDagInstance().getId(),
            workFlowTaskInstance.getWorkFlowDagInstance().getBizIdentity(),
            workFlowTaskInstance.getWorkFlowDagInstance().getRelatedId());
    } else {
        // Step 4b: 正常执行任务
        workFlowTaskResult = taskRunner.process(workFlowTaskInstance, runtimeContext);
    }

    // Step 5: 处理任务结果
    // 注意：Runner返回的状态可能是POLLING等中间状态
    // 但对于工作流的任务节点，我们需要将中间状态内部消化，只对外透出：处理中、成功、失败
    switch (workFlowTaskResult.getStatus()) {
        case SUCCESS:
            return WorkFlowTaskNodeStatusEnum.SUCCESS;

        case FAIL:
            // 处理失败，检查是否能重试/重跑
            return handlerFail(taskRunner, workFlowTaskInstance,
                workFlowTaskResult, runtimeContext);

        case POLLING:
            // 处理轮询状态
            return handlerPolling(taskRunner, workFlowTaskInstance, runtimeContext);

        case CONTINUE:
            // 继续处理（无延迟）
            return handlerContinue(runtimeContext);

        case QUEUE_PROCESSING:
            // 队列处理中
            return WorkFlowTaskNodeStatusEnum.QUEUE_PROCESSING;

        case DOING_FAIL_DIAGNOSIS:
            // 执行失败诊断中
            return handlePollingDiagnose(taskRunner, workFlowTaskInstance, runtimeContext);

        case FINISH_FAIL_DIAGNOSIS:
            // 诊断完成
            return handleFinishDiagnose(taskRunner, workFlowTaskInstance,
                workFlowTaskResult, runtimeContext);

        case LATER_CONTINUE:
            // 延迟继续
            return handlerLaterContinue(runtimeContext);

        case WORKFLOW_DISUSE:
            // 工作流被停用
            cancelTaskNoException(taskRunner, workFlowTaskInstance, runtimeContext);
            return WorkFlowTaskNodeStatusEnum.WORKFLOW_DISUSE;

        default:
            throw new IllegalStateException("不支持的工作流任务返回状态");
    }
}
```

### 4.2 handlerFail() - 重试/重跑决策

**流程**: FAIL → 重跑? → 重试? → 最终FAIL

```java
private WorkFlowTaskNodeStatusEnum handlerFail(ITaskRunner taskRunner,
                                              WorkFlowTaskInstance workFlowTaskInstance,
                                              WorkFlowTaskResult workFlowTaskResult,
                                              WorkFlowRuntimeContext runtimeContext) {
    String stepName = workFlowTaskInstance.getStepName();

    // ============ 第一优先级：重跑 ============
    WorkFlowTaskReRunStrategy reRunStrategy =
        WorkFlowTaskReRunStrategy.buildStrategy(
            runtimeContext.getTaskRuntimeContext().getReRunStrategy()
        );

    // 检查是否能重跑（有策略 + 错误类型可重跑）
    if (reRunStrategy != null &&
        reRunStrategy.canReRun(workFlowTaskResult.getWorkFlowTaskErrorTypeEnum())) {

        LOGGER.info("stepName:{},重新执行runner", stepName);
        runtimeContext.getInstanceRuntimeContext()
            .appendDigest(stepName + ":重新执行runner");

        // 取消当前任务（释放资源）
        cancelTaskNoException(taskRunner, workFlowTaskInstance, runtimeContext);

        // 返回PROCESSING，表示重新执行
        WorkFlowTaskNodeStatusEnum resultStatus = WorkFlowTaskNodeStatusEnum.PROCESSING;

        // 设置延迟
        if (reRunStrategy.retryIntervalMills() > 0) {
            runtimeContext.getTaskRuntimeContext()
                .setAppointExecuteTimeMills(
                    System.currentTimeMillis() + reRunStrategy.retryIntervalMills()
                );
        }

        // 更新重跑策略（减少计数）
        if (reRunStrategy.getNextReRunStrategy() != null) {
            runtimeContext.getTaskRuntimeContext()
                .setReRunStrategy(reRunStrategy.getNextReRunStrategy().getConfigModel());
        } else {
            runtimeContext.getTaskRuntimeContext().setReRunStrategy(null);
        }

        // 重置重试策略（因为重跑就是重新执行，要重置内部的重试计数）
        WorkFlowTaskRetryStrategy retryStrategy =
            WorkFlowTaskRetryStrategy.buildStrategy(
                runtimeContext.getTaskRuntimeContext().getRetryStrategy()
            );
        if (retryStrategy != null) {
            runtimeContext.getTaskRuntimeContext()
                .setRetryStrategy(
                    new WorkFlowTaskRetryModel(
                        retryStrategy.getConfigModel().getTotalTimes(),
                        retryStrategy.getConfigModel().getTotalTimes(),
                        retryStrategy.getConfigModel().getIntervalMills()
                    )
                );
        }

        // 重置轮询策略
        WorkFlowTaskPollingStrategy pollingStrategy =
            WorkFlowTaskPollingStrategy.buildStrategy(
                runtimeContext.getTaskRuntimeContext().getPollingStrategy()
            );
        if (pollingStrategy != null) {
            runtimeContext.getTaskRuntimeContext()
                .setPollingStrategy(
                    new WorkFlowTaskPollingModel(
                        pollingStrategy.getConfigModel().getTotalTimes(),
                        pollingStrategy.getConfigModel().getTotalTimes(),
                        pollingStrategy.getConfigModel().getIntervalMills()
                    )
                );
        }

        return resultStatus;
    }

    // ============ 第二优先级：重试 ============
    WorkFlowTaskRetryStrategy retryStrategy =
        WorkFlowTaskRetryStrategy.buildStrategy(
            runtimeContext.getTaskRuntimeContext().getRetryStrategy()
        );

    // 检查是否能重试
    if (retryStrategy != null &&
        retryStrategy.canRetry(workFlowTaskResult.getWorkFlowTaskErrorTypeEnum())) {

        LOGGER.info("stepName:{},继续重试", stepName);
        runtimeContext.getInstanceRuntimeContext()
            .appendDigest(stepName + ":继续重试");

        // 返回NODE_RETRY_PROCESSING，表示同步重试（不取消任务）
        WorkFlowTaskNodeStatusEnum resultStatus = WorkFlowTaskNodeStatusEnum.NODE_RETRY_PROCESSING;

        // 设置延迟
        if (retryStrategy.retryIntervalMills() > 0) {
            runtimeContext.getTaskRuntimeContext()
                .setAppointExecuteTimeMills(
                    System.currentTimeMillis() + retryStrategy.retryIntervalMills()
                );
        }

        // 更新重试策略（减少计数）
        if (retryStrategy.getNextRetryStrategy() != null) {
            runtimeContext.getTaskRuntimeContext()
                .setRetryStrategy(retryStrategy.getNextRetryStrategy().getConfigModel());
        } else {
            runtimeContext.getTaskRuntimeContext().setRetryStrategy(null);
        }

        return resultStatus;
    }

    // ============ 最后：都不能，最终失败 ============
    Cat.logEvent("WorkFlowTaskRunnerErr", stepName + "_执行失败终止");
    LOGGER.warn("stepName:{},执行失败终止", stepName);

    // 取消任务
    cancelTaskNoException(taskRunner, workFlowTaskInstance, runtimeContext);

    // 添加错误信息到运行时上下文
    WorkFlowProcessUtils.addErrorMsgToRuntimeContext(
        stepName,
        runtimeContext,
        workFlowTaskResult.getErrorMsg()
    );

    // 记录日志
    runtimeContext.getInstanceRuntimeContext()
        .appendDigest(stepName + ":执行失败终止");

    return WorkFlowTaskNodeStatusEnum.FAIL;
}
```

**关键决策树**:
```
FAIL
  ├─► canReRun? (重跑优先级更高)
  │   ├─YES ─► PROCESSING (重新执行)
  │   │        ├─ 取消任务
  │   │        ├─ 重置重试、轮询、重跑策略
  │   │        └─ 设置延迟
  │   │
  │   └─NO
  │       └─► canRetry? (其次是重试)
  │           ├─YES ─► NODE_RETRY_PROCESSING
  │           │        ├─ 不取消任务（同步重试）
  │           │        └─ 设置延迟
  │           │
  │           └─NO
  │               └─► FAIL (最终失败)
  │                   ├─ 取消任务
  │                   ├─ 设置失败状态
  │                   └─ 记录摘要日志
```

### 4.3 handlerPolling() - 轮询状态处理

**流程**: POLLING → 超时? → 继续轮询? → 返回WAITING_NODE_RESULT

```java
private WorkFlowTaskNodeStatusEnum handlerPolling(ITaskRunner taskRunner,
                                                  WorkFlowTaskInstance workFlowTaskInstance,
                                                  WorkFlowRuntimeContext runtimeContext) {
    String stepName = workFlowTaskInstance.getStepName();

    // Step 1: 获取轮询策略
    WorkFlowTaskPollingStrategy pollingStrategy =
        WorkFlowTaskPollingStrategy.buildStrategy(
            runtimeContext.getTaskRuntimeContext().getPollingStrategy()
        );

    // Step 2: 检查是否轮询超时
    if (pollingStrategy == null || pollingStrategy.isStop()) {
        // 超时了
        Cat.logEvent("WorkFlowTaskRunnerErr", stepName + "_超时未完成");
        LOGGER.warn("stepName:{},轮训策略结束,超时未完成", stepName);

        // 取消当前任务
        cancelTaskNoException(taskRunner, workFlowTaskInstance, runtimeContext);

        // 记录日志
        runtimeContext.getInstanceRuntimeContext()
            .appendDigest(stepName + ":轮训策略结束,超时未完成");

        // 添加错误信息
        WorkFlowProcessUtils.addErrorMsgToRuntimeContext(
            stepName,
            runtimeContext,
            stepName + "轮训策略结束,超时未完成"
        );

        // 返回FAIL
        return WorkFlowTaskNodeStatusEnum.FAIL;
    }

    // Step 3: 轮询还未超时，继续轮询
    WorkFlowTaskNodeStatusEnum resultStatus = WorkFlowTaskNodeStatusEnum.WAITING_NODE_RESULT;

    // 设置下次执行时间
    if (pollingStrategy.pollingIntervalMills() > 0) {
        runtimeContext.getTaskRuntimeContext()
            .setAppointExecuteTimeMills(
                System.currentTimeMillis() + pollingStrategy.pollingIntervalMills()
            );
    }

    // Step 4: 更新轮询策略（减少计数）
    if (pollingStrategy.getNextPollingStrategy() != null) {
        runtimeContext.getTaskRuntimeContext()
            .setPollingStrategy(pollingStrategy.getNextPollingStrategy().getConfigModel());
    } else {
        runtimeContext.getTaskRuntimeContext().setPollingStrategy(null);
    }

    return resultStatus;
}
```

---

## 第五部分：关键策略类解析

### 5.1 WorkFlowTaskRetryModel - 重试策略

```java
public class WorkFlowTaskRetryModel {
    private Integer totalTimes;      // 总重试次数
    private Integer currentTimes;    // 当前剩余次数（递减）
    private Long intervalMills;      // 重试间隔（毫秒）

    // 使用示例：retryModel = new WorkFlowTaskRetryModel(3, 3, 3000)
    // 含义：总共重试3次，每次间隔3秒
}

// 使用场景：
// 第一次失败 → canRetry = true, currentTimes = 3
// 等待3秒后重试一次
// 第二次失败 → canRetry = true, currentTimes = 2（已减少）
// 等待3秒后重试一次
// 第三次失败 → canRetry = true, currentTimes = 1（已减少）
// 等待3秒后重试一次
// 第四次失败 → canRetry = false, currentTimes = 0（已耗尽）
// 进入重跑或最终失败
```

### 5.2 WorkFlowTaskReRunModel - 重跑策略

```java
public class WorkFlowTaskReRunModel {
    private Integer totalTimes;      // 总重跑次数
    private Integer currentTimes;    // 当前剩余次数（递减）
    private Long intervalMills;      // 重跑间隔（毫秒）

    // 使用示例：reRunModel = new WorkFlowTaskReRunModel(3, 60000)
    // 含义：总共重跑3次，每次间隔60秒
}

// 重跑与重试的区别：
// 重跑: 取消当前任务，重新从头执行，重置重试/轮询计数
// 重试: 同步重试，不取消任务，仅重试计数递减
```

### 5.3 WorkFlowTaskPollingModel - 轮询策略

```java
public class WorkFlowTaskPollingModel {
    private Integer totalTimes;      // 总轮询次数
    private Integer currentTimes;    // 当前剩余次数（递减）
    private Long intervalMills;      // 轮询间隔（毫秒）

    // 使用示例：pollingModel = new WorkFlowTaskPollingModel(120, 60000)
    // 含义：最多轮询120次，每次间隔60秒 = 2小时
}

// 轮询流程：
// 第一次执行 → return POLLING
// → 等待60秒，currentTimes = 120
// 第二次执行 → 检查结果是否完成
// → return POLLING
// → 等待60秒，currentTimes = 119
// ...
// 第120次执行 → 检查结果
// → 如果还未完成，currentTimes = 0，超时返回FAIL
```

---

## 总结

### 三层处理架构

1. **消息消费层** (WorkFlowConsumerService)
   - 入口验证
   - 基础检查
   - 调用核心服务

2. **编排层** (WorkFlowService, WorkFlowProcessor)
   - 业务逻辑编排
   - 状态管理
   - 分布式协调

3. **执行层** (LocalTaskProcessor, ITaskRunner)
   - 具体任务执行
   - 重试/重跑/轮询
   - 结果处理

### 关键流程特点

- **自旋优化**: WorkFlowService中的for循环在单个消息中可处理多个步骤
- **分布式锁**: 防止多消费者并发处理同一实例
- **策略编排**: 三层策略（重跑 > 重试 > 轮询）按优先级执行
- **异常处理**: 捕获异常设置FAILURE状态并执行回调
- **延迟消息**: 通过Mafka延迟消息实现轮询和重试

### 性能优化

- 快速检查（终态、环境）避免锁竞争
- 自旋循环减少消息投递
- 2秒Sleep确保从库同步
- 60秒以下的延迟直接发送，避免Mafka成本

