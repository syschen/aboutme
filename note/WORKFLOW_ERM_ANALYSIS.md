# 工作流运行机制 - 实体关系模型分析

## 文档概述
本文档基于 `WorkFlowConsumerService` 作为入口，分析整个工作流运行机制，并用实体关系模型（Entity Relationship Model）总结核心数据结构和处理流程。

---

## 第一部分：核心实体关系模型

### 1. 实体关系图（ERM）

```
┌─────────────────────────────────────────────────────────────────┐
│                    工作流运行机制整体架构                        │
└─────────────────────────────────────────────────────────────────┘

                           消息入口层
                               │
                    ┌──────────▼──────────┐
                    │ WorkFlowConsumerService
                    │ (Mafka消息消费)     │
                    └──────────┬──────────┘
                               │
                      ┌────────▼────────┐
                      │ WorkFlowMessage │ ◄──── 解析
                      └────────┬────────┘
                               │
            ┌──────────────────▼──────────────────┐
            │  CustomWorkFlowInstanceEntity       │
            │  (工作流实例数据库持久化)          │
            └──────────────────┬──────────────────┘
                               │
                ┌──────────────▼──────────────┐
                │   WorkFlowService           │
                │   (核心处理编排器)         │
                └──────────────┬──────────────┘
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
     ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
     │ 状态验证    │   │ 环境验证    │   │ 分布式锁    │
     └─────────────┘   └─────────────┘   └─────────────┘
            │
    ┌───────▼────────────────────┐
    │ WorkFlowProcessor           │
    │ (推进工作流流程)           │
    └───────┬────────────────────┘
            │
    ┌───────▼─────────────────────────────────────┐
    │ LocalTaskProcessor                          │
    │ (本地任务执行)                             │
    ├─────────────────────────────────────────────┤
    │ - 任务路由                                  │
    │ - 重试管理                                  │
    │ - 轮询策略                                  │
    │ - 失败诊断                                  │
    └───────┬─────────────────────────────────────┘
            │
    ┌───────▼─────────────────────────────────────┐
    │ ITaskRunner (实现类)                        │
    │ - XXXExecuteRunner                          │
    │ - FewshotMLPEvalRunner                      │
    │ - 其他业务特定的Runner                     │
    └─────────────────────────────────────────────┘
```

---

## 第二部分：核心实体详解

### 1. **CustomWorkFlowInstanceEntity**（工作流实例）

**职责**: 持久化工作流执行状态

| 字段 | 类型 | 说明 | 备注 |
|------|------|------|------|
| `id` | Long | 实例ID | 主键，唯一标识一个工作流实例 |
| `bizIdentity` | String | 业务身份 | 如：`FEWSHOT_MODEL_TRAIN`、`MODEL_DEPLOY` |
| `templateName` | String | 模板名称 | 工作流模板名称 |
| `templateVersion` | String | 模板版本 | 工作流模板版本 |
| `currentStepName` | String | 当前步骤名 | 正在执行的步骤 |
| `status` | String | 实例状态 | INIT → PROCESSING → FINISHED/FAILURE/DISUSE |
| `subStatus` | String | 子状态 | 用于状态机的子状态转移 |
| `executeDag` | String | 执行DAG | JSON格式，包含所有步骤定义 |
| `taskNodeSnapshot` | String | 任务快照 | JSON格式，当前任务运行时上下文的快照 |
| `digestLog` | String | 摘要日志 | 执行过程的日志记录 |
| `runtimeContext` | String | 运行上下文 | JSON格式，业务相关的运行时数据 |
| `relatedId` | String | 关联ID | 关联的业务实体ID |
| `addTime` | Timestamp | 创建时间 | 实例创建时间 |
| `updateTime` | Timestamp | 修改时间 | 最后修改时间 |

**状态转移流程**:
```
INIT (初始化)
  │
  ▼
PROCESSING (处理中)
  │
  ├─► FINISHED (完成) ◄─ 成功路径
  │
  ├─► FAILURE (失败) ◄─ 失败路径
  │
  └─► DISUSE (废弃) ◄─ 中止路径
```

---

### 2. **WorkFlowDagInstance**（工作流DAG实例）

**职责**: 表示工作流的流程定义

```
WorkFlowDagInstance
├── id: Long (实例ID，与Entity.id对应)
├── bizIdentity: WorkFlowBizIdentityEnum (业务身份枚举)
├── status: String (当前状态)
├── subStatus: String (子状态)
├── relatedId: String (关联ID)
└── template: WorkFlowDagTemplateDefine (DAG模板定义)
    ├── name: String (模板名称)
    ├── version: String (版本)
    ├── startStep: String (起始步骤)
    └── workFlowStepNodeMap: Map<String, WorkFlowStepNodeDefine>
        └── WorkFlowStepNodeDefine (每个步骤定义)
            ├── name: String (步骤名称)
            ├── runnerName: String (运行器名称)
            ├── nextStepOnSuccess: String (成功时的下一步)
            ├── retryModel: WorkFlowTaskRetryModel (重试策略)
            ├── reRunModel: WorkFlowTaskReRunModel (重跑策略)
            ├── pollingModel: WorkFlowTaskPollingModel (轮询策略)
            ├── pollingDiagnosisModel: WorkFlowTaskPollingModel (诊断轮询策略)
            ├── configParams: Map<String, Object> (配置参数)
            └── checkSuccessTimes: Integer (成功检查次数)
```

---

### 3. **WorkFlowTaskInstance**（工作流任务实例）

**职责**: 代表当前执行的任务

```
WorkFlowTaskInstance
├── stepName: String (步骤名称)
├── workFlowDagInstance: WorkFlowDagInstance (所属工作流DAG)
├── workFlowStepNodeDefine: WorkFlowStepNodeDefine (步骤定义)
└── getConfigParams(): Map<String, Object> (获取步骤配置参数)
```

---

### 4. **WorkFlowRuntimeContext**（工作流运行时上下文）

**职责**: 保存工作流执行过程中的运行时数据

```
WorkFlowRuntimeContext
├── taskRuntimeContext: TaskRuntimeContext (当前任务的运行时上下文)
│   ├── retryStrategy: WorkFlowTaskRetryModel (重试策略状态)
│   ├── reRunStrategy: WorkFlowTaskReRunModel (重跑策略状态)
│   ├── pollingStrategy: WorkFlowTaskPollingModel (轮询策略状态)
│   ├── pollingDiagnosisModel: WorkFlowTaskPollingModel (诊断轮询策略)
│   ├── appointExecuteTimeMills: Long (指定执行时间)
│   ├── checkSuccessTimes: Integer (剩余成功检查次数)
│   ├── startExecuteTimeMills: Long (开始执行时间)
│   └── taskNodeStatusEnum: WorkFlowTaskNodeStatusEnum (任务节点状态)
│
└── instanceRuntimeContext: ProcessInstanceRuntimeContext (流程实例运行时上下文)
    ├── digestLog: String (摘要日志)
    └── runContext: Map<String, Object> (执行上下文)
        ├── EVAL_RECORD_ID: String
        ├── EXPERIMENT_TASK_ID: String
        ├── EXECUTE_ENV: String
        ├── EXECUTE_CELL: String
        ├── OPERATOR: String
        ├── TENANT_ID: String
        └── ... (其他业务相关数据)
```

---

### 5. **WorkFlowTaskResult**（工作流任务执行结果）

**职责**: 表示单个任务的执行结果

```
WorkFlowTaskResult
├── status: WorkFlowTaskResultStatusEnum
│   ├── SUCCESS (成功)
│   ├── FAIL (失败)
│   ├── POLLING (轮询中)
│   ├── CONTINUE (继续处理)
│   ├── LATER_CONTINUE (延迟继续)
│   ├── QUEUE_PROCESSING (队列处理中)
│   ├── DOING_FAIL_DIAGNOSIS (执行失败诊断中)
│   └── FINISH_FAIL_DIAGNOSIS (失败诊断完成)
│
├── result: Object (任务执行结果数据)
├── errorMsg: String (错误信息)
├── errorCauseInfo: String (错误原因)
└── workFlowTaskErrorTypeEnum: WorkFlowTaskErrorTypeEnum (错误类型)
```

---

## 第三部分：流程处理流程

### 1. **消息消费流程** → WorkFlowConsumerService

```
消息入口
   │
   ▼
receiveWorkFlowData(String workFlowData)
   │
   ├─► 非空验证 ✗ → 返回true（ACK消息）
   │
   ├─► 解析为WorkFlowMessage
   │
   ├─► 业务标识黑名单检查
   │
   └─► processWorkFlowMessage(WorkFlowMessage)
       │
       ├─► 获取instanceId
       │
       ├─► 查询CustomWorkFlowInstanceEntity
       │
       ├─► 检查实例状态
       │   ├─► 终态 → 跳过处理 ✗
       │   └─► 非终态 → 继续
       │
       └─► workFlowService.process(entity)
           │
           └─► [进入核心处理流程] ✓
```

---

### 2. **核心处理流程** → WorkFlowService

```
process(CustomWorkFlowInstanceEntity entity)
   │
   ├─► 状态检查
   │   └─► 已是终态 → 返回 ✗
   │
   ├─► 环境检查
   │   └─► 执行环境/Set不匹配 → 返回 ✗
   │
   ├─► 分布式锁竞争
   │   └─► 获取失败 → 返回 ✗
   │
   ├─► for(;;) 自旋处理 [锁定期间]
   │   │
   │   ├─► 重新查询最新实体
   │   │
   │   ├─► 更新状态 (INIT → PROCESSING)
   │   │
   │   ├─► 构建领域模型
   │   │   ├─► WorkFlowDagInstance (从executeDag JSON)
   │   │   ├─► WorkFlowTaskInstance (当前步骤)
   │   │   └─► WorkFlowRuntimeContext (从taskNodeSnapshot和runtimeContext)
   │   │
   │   ├─► 检查是否需要停止
   │   │   └─► 终态 → 返回 ✗
   │   │
   │   ├─► 检查是否需要等待
   │   │   ├─► appointExecuteTimeMills未到 → 发延迟消息 + 返回 ✗
   │   │   └─► 时间已到 → 继续
   │   │
   │   ├─► 推进一步
   │   │   └─► workFlowProcessor.pushOneStep(...)
   │   │       │
   │   │       └─► [进入任务处理流程]
   │   │
   │   ├─► Thread.sleep(2000) [等待数据库从库同步]
   │   │
   │   ├─► 检查返回的任务状态
   │   │   ├─► LATER_CONTINUE_PROCESSING → 返回 ✗
   │   │   ├─► WAITING_NODE_RESULT → 返回 ✗
   │   │   └─► 其他状态 → 继续循环
   │   │
   │   └─► [异常处理] 设置FAILURE状态并返回
   │
   └─► 最后释放分布式锁
```

---

### 3. **任务推进流程** → WorkFlowProcessor

```
pushOneStep(WorkFlowTaskInstance, WorkFlowRuntimeContext)
   │
   ├─► taskProcess(...)
   │   │
   │   ├─► 获取ITaskProcessor (默认LocalTaskProcessor)
   │   │
   │   ├─► 初始化startExecuteTimeMills
   │   │
   │   ├─► 调用processor.action(RUN, ...)
   │   │
   │   └─► 返回WorkFlowTaskNodeStatusEnum
   │
   ├─► 根据返回状态处理
   │   │
   │   ├─► SUCCESS
   │   │   └─► onSuccessStatus(...)
   │   │       ├─► 记录指标
   │   │       ├─► 清除taskRuntimeContext
   │   │       ├─► 获取nextStepOnSuccess
   │   │       ├─► 无下一步 → 设置FINISHED + 执行成功回调
   │   │       └─► 有下一步 → 更新stepName
   │   │
   │   ├─► FAIL / FINISH_FAIL_DIAGNOSIS
   │   │   └─► onFailStatus(...)
   │   │       ├─► 记录指标
   │   │       ├─► 设置FAILURE状态
   │   │       ├─► 状态机转移
   │   │       └─► 执行失败回调
   │   │
   │   ├─► WORKFLOW_DISUSE
   │   │   └─► onWorkFlowDisuseStatus(...)
   │   │       ├─► 设置DISUSE状态
   │   │       └─► 执行停用回调
   │   │
   │   └─► 其他中间状态 (PROCESSING, WAITING, etc.)
   │       └─► 不做处理，返回状态
   │
   ├─► 将领域模型转换为Entity
   │   └─► WorkFlowModelHelper.convertWorkFlowEntity(...)
   │
   ├─► 更新数据库
   │   └─► workFlowModelService.updateById(entity)
   │
   └─► 返回WorkFlowTaskNodeStatusEnum
```

---

### 4. **本地任务处理流程** → LocalTaskProcessor

```
action(WorkFlowTaskInstance, WorkFlowTaskActionEnum.RUN, WorkFlowRuntimeContext)
   │
   └─► taskRun(...)
       │
       ├─► 获取ITaskRunner (工厂模式获取)
       │
       ├─► 检查是否被停止
       │   └─► 是 → taskRunner.cancel() + 返回TASK_STOP
       │
       ├─► 执行任务
       │   └─► taskRunner.process(...)
       │       └─► 返回WorkFlowTaskResult
       │
       └─► 根据result.status分类处理
           │
           ├─► SUCCESS
           │   └─► 返回 WorkFlowTaskNodeStatusEnum.SUCCESS
           │
           ├─► FAIL
           │   └─► handlerFail(...) 处理重试/重跑策略
           │       │
           │       ├─► 能重跑 → 返回 PROCESSING
           │       ├─► 能重试 → 返回 NODE_RETRY_PROCESSING
           │       └─► 都不能 → 返回 FAIL
           │
           ├─► POLLING
           │   └─► handlerPolling(...) 处理轮询
           │       │
           │       ├─► 轮询超时 → 返回 FAIL
           │       └─► 轮询继续 → 设置下次执行时间 + 返回 WAITING_NODE_RESULT
           │
           ├─► CONTINUE
           │   └─► 返回 CONTINUE_PROCESSING
           │
           ├─► LATER_CONTINUE
           │   └─► 返回 LATER_CONTINUE_PROCESSING
           │
           ├─► QUEUE_PROCESSING
           │   └─► 返回 QUEUE_PROCESSING
           │
           ├─► DOING_FAIL_DIAGNOSIS
           │   └─► handlePollingDiagnose(...) 处理诊断轮询
           │       ├─► 诊断超时 → 返回 FAIL
           │       └─► 诊断继续 → 返回 WAITING_FAIL_DIAGNOSIS_RESULT
           │
           ├─► FINISH_FAIL_DIAGNOSIS
           │   └─► handleFinishDiagnose(...) → 返回 FINISH_FAIL_DIAGNOSIS
           │
           └─► WORKFLOW_DISUSE
               └─► taskRunner.cancel() → 返回 WORKFLOW_DISUSE
```

---

## 第四部分：关键策略模型

### 1. **重试策略** (WorkFlowTaskRetryModel)

```
WorkFlowTaskRetryModel
├── totalTimes: Integer (总重试次数)
├── currentTimes: Integer (当前剩余次数)
└── intervalMills: Long (重试间隔，毫秒)

执行流程:
  ├─► 任务失败
  ├─► 检查是否能重试 (currentTimes > 0 且 errorType可重试)
  ├─► 能重试 → 减少currentTimes + 设置延迟 → PROCESSING
  └─► 不能重试 → FAIL
```

### 2. **重跑策略** (WorkFlowTaskReRunModel)

```
WorkFlowTaskReRunModel
├── totalTimes: Integer (总重跑次数)
├── currentTimes: Integer (当前剩余次数)
└── intervalMills: Long (重跑间隔，毫秒)

执行流程:
  ├─► 任务失败
  ├─► 检查是否能重跑 (currentTimes > 0 且 errorType可重跑)
  ├─► 能重跑 → 取消任务 + 重置重试/轮询策略 → PROCESSING
  └─► 不能重跑 → 进入重试策略
```

### 3. **轮询策略** (WorkFlowTaskPollingModel)

```
WorkFlowTaskPollingModel
├── totalTimes: Integer (总轮询次数)
├── currentTimes: Integer (当前剩余次数)
└── intervalMills: Long (轮询间隔，毫秒)

执行流程:
  ├─► 任务返回POLLING状态
  ├─► 检查是否轮询超时 (currentTimes <= 0)
  ├─► 超时 → 取消任务 → FAIL
  ├─► 未超时 → 减少currentTimes + 设置下次执行时间 → WAITING_NODE_RESULT
  └─► [下一轮消息触发重新执行]
```

---

## 第五部分：实体生命周期

### 工作流实例完整生命周期

```
1. 创建阶段
   └─► 构建 CustomWorkFlowInstanceEntity
       ├─► 状态: INIT
       ├─► 子状态: INIT
       ├─► currentStepName: 起始步骤
       ├─► executeDag: 工作流模板JSON
       ├─► taskNodeSnapshot: 空
       └─► 保存到数据库

2. 消息触发阶段
   └─► Mafka消费WorkFlowMessage
       └─► WorkFlowConsumerService.receiveWorkFlowData(...)

3. 处理阶段
   └─► WorkFlowService.process(entity)
       │
       ├─► [第一次循环]
       │   ├─► 更新状态 INIT → PROCESSING
       │   ├─► 获取第一个步骤
       │   ├─► 执行任务
       │   ├─► 根据结果更新状态/stepName
       │   └─► 保存到数据库
       │
       ├─► [后续循环] (如需继续)
       │   ├─► 消息再次触发
       │   ├─► 获取当前步骤
       │   ├─► 执行任务
       │   └─► 保存到数据库
       │
       └─► [循环直到] 终态 (FINISHED/FAILURE/DISUSE)

4. 终止阶段
   └─► 状态转移到终态
       ├─► 执行对应回调
       ├─► 记录摘要日志
       └─► 实例完成
```

---

## 第六部分：关键流程决策点

### 1. 状态检查决策

```
     START
       │
       ▼
   ┌─────────────────────┐
   │ 实例是否已终态?      │
   └────┬────────────────┘
        │ YES
        ▼
   [返回，不处理]

        │ NO
        ▼
   ┌─────────────────────┐
   │ 执行环境是否匹配?    │
   └────┬────────────────┘
        │ NO
        ▼
   [返回，不处理]

        │ YES
        ▼
   ┌─────────────────────┐
   │ 能否获取分布式锁?    │
   └────┬────────────────┘
        │ NO
        ▼
   [返回，不处理]

        │ YES
        ▼
   [开始处理]
```

### 2. 任务执行后的决策

```
   任务执行返回 WorkFlowTaskResult
       │
       ▼
   ┌─────────────────────────┐
   │ 返回状态是什么?          │
   └─┬──────────────────────┬┬┬────┐
     │                      │││    │
    成功              失败  轮询  其他
     │                │      │     │
     ▼                ▼      ▼     ▼
   获取下一步    重试重跑   继续轮  中间状态
   是否存在?     策略       询    不处理
     │
  ├─YES       ├─能重跑    ├─超时
  │           │           │
  ▼           ▼           ▼
更新步骤    PROCESSING   FAIL
  NEXT       重置策略

  └─NO
    │
    ▼
  设置FINISHED
  执行回调
```

---

## 第七部分：关键优化设计

### 1. **分布式锁机制**
- **目的**: 防止多个消费者并发处理同一实例
- **实现**: Redis分布式锁
- **锁超时**: 可配置（默认10分钟）
- **循环获取**: 同一轮消息中获取失败后直接返回

### 2. **数据库延迟同步处理**
- **问题**: 数据库主从延迟可能导致读取过期数据
- **解决**: 更新后 Thread.sleep(2000) 确保从库同步
- **作用**: 下一轮消息处理时能获取最新状态

### 3. **任务快照机制**
- **作用**: 保存当前任务的运行时状态（重试次数、轮询计数等）
- **恢复**: 宕机后可从快照恢复到中断点继续执行

### 4. **延迟消息处理**
- **场景**: 任务需要延迟执行或轮询等待
- **实现**: 计算延迟秒数，发送延迟消息
- **优势**: 避免忙轮询，节省资源

### 5. **环境/Set隔离**
- **作用**: 区分不同环境的工作流执行
- **检查**: EXECUTE_ENV 和 EXECUTE_CELL 必须匹配

---

## 第八部分：工作流模板示例

### ExperimentTestWorkFlowTemplate 结构示例

```
workFlowStepNodeMap:
  │
  ├─► "experimentTestInputExecuteRunner"
  │   ├─► nextStepOnSuccess: "experimentTestMlpExecuteRunner"
  │   ├─► retryModel: (3次，每次3秒)
  │   ├─► reRunModel: (3次，每次60秒)
  │   └─► pollingModel: (120次，每次60秒)
  │
  ├─► "experimentTestMlpExecuteRunner"  [轮询任务]
  │   ├─► nextStepOnSuccess: "experimentTestOutputExecuteRunner"
  │   ├─► pollingModel: (1080次，每次60秒) [约18小时]
  │   └─► ...
  │
  ├─► "experimentTestOutputExecuteRunner"
  │   ├─► nextStepOnSuccess: "experimentTestHbaseExecuteRunner"
  │   └─► ...
  │
  ├─► "experimentTestHbaseExecuteRunner"
  │   ├─► nextStepOnSuccess: "experimentTestResultCsvProcessRunner"
  │   └─► ...
  │
  └─► "experimentTestResultCsvProcessRunner"  [最后一步]
      ├─► nextStepOnSuccess: null
      └─► retryModel: (3次，每次3秒)
```

---

## 第九部分：错误处理流程

### 任务执行异常处理

```
processWorkFlowMessage 中的异常
    │
    ▼
设置状态为 FAILURE
设置子状态为 FORCED_FAIL
    │
    ▼
追加错误日志到 digestLog
    │
    ▼
执行失败回调
(workFlowCallbackManager.executeErrorCallbacks)
    │
    ▼
更新数据库
    │
    ▼
抛出异常 (MafkaAck失败，消息重新投递)
```

---

## 第十部分：核心类职责总结

| 类名 | 职责 | 关键方法 |
|------|------|--------|
| `WorkFlowConsumerService` | Mafka消息消费入口 | `receiveWorkFlowData()` |
| `WorkFlowService` | 工作流处理编排器 | `process()` |
| `WorkFlowProcessor` | 单步推进处理器 | `pushOneStep()` |
| `LocalTaskProcessor` | 本地任务执行 | `action()` / `taskRun()` |
| `ITaskRunner` | 具体业务任务执行 | `process()` / `cancel()` |
| `WorkFlowModelHelper` | 领域模型构建 | `buildWorkFlowDagDefine()` / `buildWorkFlowRuntime()` |
| `WorkFlowCallbackManager` | 状态回调管理 | `executeCallbacks()` |
| `WorkFlowMessageSender` | 消息发送管理 | `delaySendWorkFlowMessage()` |

---

## 总结

### 工作流运行机制核心要素

1. **入口**: Mafka消息消费 (`WorkFlowConsumerService`)
2. **持久化**: 数据库实体 (`CustomWorkFlowInstanceEntity`)
3. **并发控制**: 分布式锁 (`RedisLockGateway`)
4. **编排**: 工作流处理器 (`WorkFlowService`, `WorkFlowProcessor`)
5. **执行**: 任务执行器 (`LocalTaskProcessor`, `ITaskRunner`)
6. **策略**: 重试、重跑、轮询、诊断
7. **状态**: 状态机转移 (INIT → PROCESSING → 终态)
8. **上下文**: 运行时上下文 (任务级 + 实例级)
9. **持久化**: 快照机制 (taskNodeSnapshot)
10. **异步**: 延迟消息 (后续处理)

### 关键特点

- ✅ **高可用**: 分布式锁 + 消息重投
- ✅ **容错性**: 重试/重跑/轮询机制
- ✅ **可观测**: 摘要日志 + 执行指标
- ✅ **隔离性**: 环境/Set隔离
- ✅ **灵活性**: 模板化定义 + Runner插件化

