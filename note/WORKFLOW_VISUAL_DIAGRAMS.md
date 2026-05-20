# 工作流运行机制 - 可视化图表集合

## 图表目录

- [1. 数据流向图](#1-数据流向图)
- [2. 状态转移图](#2-状态转移图)
- [3. 任务执行序列图](#3-任务执行序列图)
- [4. 策略执行决策树](#4-策略执行决策树)
- [5. 实体依赖关系图](#5-实体依赖关系图)
- [6. 处理循环内部流程](#6-处理循环内部流程)

---

## 1. 数据流向图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         数据从持久化到执行的完整流向                          │
└──────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────┐
│ CustomWorkFlow      │
│ InstanceEntity      │
│ (数据库记录)        │
└──────────┬──────────┘
           │
           │ JSON反序列化
           ▼
┌─────────────────────────────────────────────┐
│ 1️⃣ WorkFlowDagInstance                      │
│    └─ template: WorkFlowDagTemplateDefine   │
│       └─ workFlowStepNodeMap                │
│          └─ 所有步骤定义                    │
└──────────┬──────────────────────────────────┘
           │ 根据currentStepName查找
           ▼
┌─────────────────────────────────────────────┐
│ 2️⃣ WorkFlowTaskInstance                     │
│    ├─ stepName                              │
│    ├─ workFlowDagInstance                   │
│    └─ workFlowStepNodeDefine                │
│       └─ runnerName                         │
└──────────┬──────────────────────────────────┘
           │ 获取或恢复
           ▼
┌─────────────────────────────────────────────┐
│ 3️⃣ WorkFlowRuntimeContext                   │
│    ├─ TaskRuntimeContext                    │
│    │  ├─ retryStrategy                      │
│    │  ├─ reRunStrategy                      │
│    │  ├─ pollingStrategy                    │
│    │  └─ appointExecuteTimeMills            │
│    │                                         │
│    └─ ProcessInstanceRuntimeContext         │
│       ├─ digestLog                          │
│       └─ runContext (Map)                   │
└──────────┬──────────────────────────────────┘
           │ 根据runnerName获取
           ▼
┌─────────────────────────────────────────────┐
│ 4️⃣ ITaskRunner                              │
│    └─ 由工厂获得的具体实现类                │
│       └─ FewshotMLPEvalRunner                │
│       └─ ExperimentTestMlpExecuteRunner      │
│       └─ ...                                 │
└──────────┬──────────────────────────────────┘
           │ 执行任务
           ▼
┌─────────────────────────────────────────────┐
│ 5️⃣ WorkFlowTaskResult                       │
│    ├─ status (SUCCESS/FAIL/POLLING/...)     │
│    ├─ result                                │
│    ├─ errorMsg                              │
│    └─ errorCauseInfo                        │
└──────────┬──────────────────────────────────┘
           │ 状态处理
           ▼
┌─────────────────────────────────────────────┐
│ 6️⃣ 决策分支                                 │
│    ├─ SUCCESS                               │
│    │  └─ 获取nextStep                      │
│    │                                         │
│    ├─ FAIL                                  │
│    │  └─ 检查重试/重跑                     │
│    │                                         │
│    ├─ POLLING                               │
│    │  └─ 检查轮询策略                      │
│    │                                         │
│    └─ 其他中间状态                          │
└──────────┬──────────────────────────────────┘
           │ 更新实体
           ▼
┌─────────────────────────────────────────────┐
│ 7️⃣ 新的CustomWorkFlowInstanceEntity         │
│    ├─ currentStepName (更新)                │
│    ├─ status (更新)                         │
│    ├─ subStatus (更新)                      │
│    ├─ taskNodeSnapshot (更新)               │
│    ├─ digestLog (更新)                      │
│    └─ runtimeContext (更新)                 │
└──────────┬──────────────────────────────────┘
           │ 保存数据库
           ▼
      数据库持久化
      (下一轮消息处理继续)
```

---

## 2. 状态转移图

### 2.1 工作流实例状态转移

```
                     ┌─────────┐
                     │  START  │
                     └────┬────┘
                          │
                          ▼
                    ┌──────────┐
                    │  INIT    │ ◄──── 实例创建后的初始状态
                    └────┬─────┘
                         │
                 [Mafka消息触发]
                         │
                         ▼
                  ┌──────────────┐
                  │  PROCESSING  │ ◄──── 实例开始处理
                  └──┬────┬────┬─┘
                     │    │    │
           ┌─────────┘    │    └──────────┐
           │              │               │
      [成功路径]      [等待路径]       [失败路径]
           │              │               │
           ▼              ▼               ▼
       ┌────────┐   ┌──────────┐   ┌────────┐
       │FINISHED│   │再发延迟  │   │FAILURE │ ◄──── 异常或重试/轮询超时
       └────────┘   │消息      │   └────────┘
                    └──────────┘
                         │
                    [下一轮处理]
                         │
                         ▼
                    ┌──────────┐
                    │PROCESSING│
                    └─────┬────┘
                          │
                    [继续处理]
                          │
                    ┌─────┴──────┐
                    │            │
                    ▼            ▼
              [最终成功]    [最终失败]
                    │            │
                    ▼            ▼
                ┌────────┐  ┌────────┐
                │FINISHED│  │FAILURE │
                └────────┘  └────────┘

特殊状态:
    DISUSE: 工作流被强制停止 (工作流级别的中止)
```

### 2.2 子状态转移 (SubStatus State Machine)

```
初始状态: INIT

成功分支:
  INIT → STEP1_OK → STEP2_OK → ... → FINISHED

失败分支:
  INIT → STEP1_OK → STEP2_FAIL → FAILURE
  或
  INIT → STEP1_FAIL → FAILURE

说明:
  - 每个步骤成功后进行状态机转移
  - 子状态用于精细化跟踪每个步骤的执行情况
  - 用于错误诊断和流程审计
```

### 2.3 任务节点状态转移

```
WorkFlowTaskNodeStatusEnum 状态图:

                    任务开始执行
                          │
                          ▼
                    ┌──────────────┐
                    │  任务执行器  │
                    └──┬───┬──┬──┬──┘
                       │   │  │  │
       ┌───────────────┘   │  │  └──────────────┐
       │                   │  │                 │
       ▼                   ▼  ▼                 ▼
   ┌────────┐          ┌──────────┐       ┌─────────┐
   │SUCCESS │          │FAIL      │       │POLLING  │
   └────────┘          └────┬─────┘       └────┬────┘
       │                    │                   │
       │           ┌────────┼────────┐          │
       │           │                 │          │
       ▼           ▼                 ▼          ▼
   下一步    能重跑?         能重试?    继续轮询
                │                 │        │
           ┌─YES ┴─NO         ┌─YES┴─NO   │
           │                 │            │
           ▼                 ▼            ▼
       PROCESSING      NODE_RETRY_PROCESSING
                                          │
                                          ▼
                                  WAITING_NODE_RESULT
                                  (等待下一轮消息)

其他状态:
    CONTINUE_PROCESSING: 继续处理（无延迟）
    LATER_CONTINUE_PROCESSING: 延迟继续处理
    QUEUE_PROCESSING: 队列处理中
    WAITING_FAIL_DIAGNOSIS_RESULT: 等待失败诊断
    FINISH_FAIL_DIAGNOSIS: 诊断完成
    WORKFLOW_DISUSE: 工作流停用
```

---

## 3. 任务执行序列图

### 3.1 单轮消息处理序列

```
消息消费端          WorkFlowConsumer    WorkFlowService    WorkFlowProcessor    TaskRunner    数据库
    │                    │                   │                   │                 │           │
    │ WorkFlowMessage    │                   │                   │                 │           │
    ├──────────────────►│                   │                   │                 │           │
    │                   │                   │                   │                 │           │
    │                   │ 解析验证          │                   │                 │           │
    │                   ├──────────────────►│                   │                 │           │
    │                   │                   │                   │                 │           │
    │                   │                   │ 获取分布式锁      │                 │           │
    │                   │                   ├─────────────────►│                 │           │
    │                   │                   │◄─────────────────┤                 │           │
    │                   │                   │   [成功]          │                 │           │
    │                   │                   │                   │                 │           │
    │                   │                   │ 查询实体          │                 │           │
    │                   │                   │──────────────────────────────────────────────►│
    │                   │                   │◄──────────────────────────────────────────────│
    │                   │                   │                   │                 │    entity │
    │                   │                   │                   │                 │           │
    │                   │                   │ 构建领域模型      │                 │           │
    │                   │                   │ (DAG/Task/Runtime)                 │           │
    │                   │                   │                   │                 │           │
    │                   │                   │ 调用推进一步      │                 │           │
    │                   │                   ├──────────────────►│                 │           │
    │                   │                   │                   │                 │           │
    │                   │                   │                   │ taskProcess     │           │
    │                   │                   │                   ├────────────────►│           │
    │                   │                   │                   │                 │           │
    │                   │                   │                   │    return       │           │
    │                   │                   │                   │◄────────────────┤           │
    │                   │                   │◄──────────────────┤ (Result)        │           │
    │                   │                   │                   │                 │           │
    │                   │                   │ 更新实体          │                 │           │
    │                   │                   ├──────────────────────────────────────────────►│
    │                   │                   │                   │                 │  update  │
    │                   │                   │                   │                 │           │
    │                   │                   │ 释放锁            │                 │           │
    │                   │                   ├─────────────────────────────────────┤           │
    │                   │                   │                   │                 │           │
    │                   │ return true       │                   │                 │           │
    │                   │◄──────────────────┤                   │                 │           │
    │                   │                   │                   │                 │           │
    │ ACK               │                   │                   │                 │           │
    │◄──────────────────┤                   │                   │                 │           │
    │                   │                   │                   │                 │           │

图例:
    ──► 方向箭头表示调用/请求
    ◄── 方向箭头表示返回/响应
```

### 3.2 任务执行流程序列

```
WorkFlowProcessor            LocalTaskProcessor         ITaskRunner
        │                            │                        │
        │ pushOneStep()              │                        │
        ├───────────────────────────►│                        │
        │                            │                        │
        │                            │ taskRun()             │
        │                            │                        │
        │                            │ 1. 检查停止状态       │
        │                            │                        │
        │                            │ 2. process()         │
        │                            ├───────────────────────►│
        │                            │                        │
        │                            │       WorkFlowTaskResult
        │                            │◄───────────────────────┤
        │                            │                        │
        │                            │ 3. 处理结果           │
        │                            │    ├─ SUCCESS         │
        │                            │    ├─ FAIL            │
        │                            │    ├─ POLLING         │
        │                            │    ├─ CONTINUE        │
        │                            │    └─ ...             │
        │                            │                        │
        │                            │ 4. 返回NodeStatus    │
        │◄───────────────────────────┤                        │
        │   WorkFlowTaskNodeStatusEnum                        │
        │                            │                        │
        │ 5. 处理状态                │                        │
        │    ├─ onSuccessStatus()   │                        │
        │    ├─ onFailStatus()      │                        │
        │    └─ onWorkFlowDisuseStatus()                     │
        │                            │                        │
        │ 6. 将结果转换为Entity     │                        │
        │ 7. 更新数据库             │                        │
        │ 8. 返回结果               │                        │
        └                            └                        └
```

---

## 4. 策略执行决策树

### 4.1 重试/重跑策略决策树

```
                    任务执行失败
                          │
                          ▼
              ┌───────────────────────────┐
              │ 能否进行重跑?             │
              │ (getReRunStrategy != null)│
              └───┬─────────────────┬──────┘
                  │                 │
             YES  │                 │  NO
                  ▼                 ▼
         ┌─────────────────┐  ┌──────────────────────┐
         │ 错误类型是否    │  │ 能否进行重试?        │
         │ 可重跑?         │  │ (getRetryStrategy    │
         └──┬──────────┬───┘  │  != null)            │
            │          │      └───┬────────────┬──────┘
       YES  │          │ NO       │            │
            ▼          ▼          │YES        │NO
      ┌────────────┐ ┌─┐         │           │
      │ 重跑计数   │ │ │         ▼           ▼
      │ > 0?      │ │ │    ┌─────────────┐ ┌─────────────┐
      └─┬────────┬┘ │ │    │ 重试计数    │ │ FAIL        │
    YES │        │  │ │    │ > 0?        │ │ (终止)      │
        ▼        ▼  │ │    └─┬────────┬──┘ └─────────────┘
    ┌────────┐ ┌─┐ │ │  YES  │        │ NO
    │PROCESSING
    │        │ │ │ │ │        ▼        ▼
    └────────┘ │ │ │ │   ┌────────────┐ ┌───────────────┐
    重置计数   │ │ │ │   │NODE_RETRY_ │ │FAIL           │
    └──────────┘ │ │ │   │PROCESSING  │ │(终止)         │
               │ │ │    └────────────┘ └───────────────┘
               └─┴─┘    重置计数
```

### 4.2 轮询策略决策树

```
                任务返回 POLLING
                      │
                      ▼
          ┌──────────────────────┐
          │ 轮询策略是否存在?    │
          │ (pollingModel != null)│
          └───┬────────────┬──────┘
              │            │
          YES │            │ NO
              ▼            ▼
         ┌─────────┐  ┌──────────┐
         │ 轮询    │  │ FAIL     │
         │ 超时?   │  │ (立即失败)
         │ (times  │  └──────────┘
         │ <= 0)   │
         └───┬──┬──┘
         YES │  │ NO
             ▼  ▼
        ┌────┐ ┌──────────────────────────────────┐
        │FAIL│ │ WAITING_NODE_RESULT              │
        │    │ │ 设置 appointExecuteTimeMills     │
        │    │ │ (下次执行时间 = 当前+轮询间隔)  │
        └────┘ │ 轮询计数 -= 1                    │
               └──────────────────────────────────┘
                           │
                           ▼
              [发送延迟消息或等待下一轮]
                           │
                           ▼
              [下一轮消息触发重新执行]
```

---

## 5. 实体依赖关系图

### 5.1 完整实体依赖关系

```
CustomWorkFlowInstanceEntity
│
├─► 1:1 ─────────────────┐
│                        │
│         ┌──────────────┘
│         │
│         ▼
│   WorkFlowDagInstance
│   ├─► 1:1 ──────────────────────────────┐
│   │                                      │
│   │           ┌───────────────────────────┘
│   │           │
│   │           ▼
│   │   WorkFlowDagTemplateDefine
│   │   ├─► 1:N ─────────────┐
│   │   │                    │
│   │   │        ┌───────────┘
│   │   │        │
│   │   │        ▼
│   │   │   WorkFlowStepNodeDefine (多个)
│   │   │   ├─► 1:1 ─► WorkFlowTaskRetryModel
│   │   │   ├─► 1:1 ─► WorkFlowTaskReRunModel
│   │   │   ├─► 1:1 ─► WorkFlowTaskPollingModel
│   │   │   ├─► 1:1 ─► WorkFlowTaskPollingModel (诊断)
│   │   │   └─► configParams: Map
│   │   │
│   │   └─► startStep: String (指向某个step)
│   │
│   ├─► 1:1 ────────────┐
│   │                   │
│   │      ┌────────────┘
│   │      │
│   │      ▼
│   │ WorkFlowBizIdentityEnum (业务身份)
│   │
│   └─► currentStepName ────┐
│                           │
└──────────────────────────┐│
                           ││
                           ▼│
WorkFlowTaskInstance        │
├─► 1:1 ──────────────────┐ │
│                        │ │
│         ┌──────────────┘ │
│         │                │
│         ▼                │
│   WorkFlowStepNodeDefine ◄┘
│
├─► 1:1 ────────────┐
│                   │
│      ┌────────────┘
│      │
│      ▼
│ WorkFlowDagInstance


WorkFlowRuntimeContext
├─► 1:1 ────────────────┐
│                       │
│        ┌──────────────┘
│        │
│        ▼
│   TaskRuntimeContext
│   ├─► retryStrategy: WorkFlowTaskRetryModel
│   ├─► reRunStrategy: WorkFlowTaskReRunModel
│   ├─► pollingStrategy: WorkFlowTaskPollingModel
│   └─► appointExecuteTimeMills: Long
│
└─► 1:1 ────────────────┐
                        │
         ┌──────────────┘
         │
         ▼
    ProcessInstanceRuntimeContext
    ├─► digestLog: String
    └─► runContext: Map<String, Object>
        ├─► EVAL_RECORD_ID
        ├─► EXPERIMENT_TASK_ID
        ├─► TENANT_ID
        ├─► OPERATOR
        └─► ...其他业务数据
```

### 5.2 处理流程中的类调用关系

```
WorkFlowConsumerService
    └─ receiveWorkFlowData()
        │
        ├─► WorkFlowModelService
        │   └─ selectById() ─► CustomWorkFlowInstanceEntity
        │
        └─► WorkFlowService
            └─ process(entity)
                │
                ├─► RedisLockGateway
                │   └─ tryLock() / unLock()
                │
                ├─► WorkFlowModelHelper
                │   ├─ buildWorkFlowDagDefine()
                │   │   └─ JSONde-serialization
                │   ├─ buildWorkFlowTaskNode()
                │   │   └─ 根据currentStepName查找步骤
                │   ├─ buildWorkFlowRuntime()
                │   │   └─ 构建运行时上下文
                │   └─ convertWorkFlowEntity()
                │       └─ 将领域模型转换为Entity
                │
                ├─► WorkFlowProcessor
                │   └─ pushOneStep()
                │       │
                │       ├─► LocalTaskProcessor
                │       │   └─ action(RUN)
                │       │       │
                │       │       ├─► TaskRunnerFactory
                │       │       │   └─ getTaskRunner(runnerName)
                │       │       │       └─ ITaskRunner (具体实现)
                │       │       │           └─ process()
                │       │       │
                │       │       └─ 处理结果:
                │       │           ├─ handlerFail()
                │       │           ├─ handlerPolling()
                │       │           └─ handlerContinue()
                │       │
                │       └─ onSuccessStatus() / onFailStatus()
                │
                ├─► WorkFlowCallbackManager
                │   └─ executeCallbacks() / executeErrorCallbacks()
                │
                ├─► WorkFlowModelService
                │   └─ updateById() ─► 保存到数据库
                │
                └─► WorkFlowMessageSender
                    └─ delaySendWorkFlowMessage()
```

---

## 6. 处理循环内部流程

### 6.1 完整的处理循环流程图

```
START: WorkFlowService.process()
    │
    │ 检查：状态是否已是终态?
    ├─YES ─► 返回
    │
    │ 检查：执行环境是否匹配?
    ├─NO ──► 返回
    │
    │ 获取分布式锁 (Redis)
    ├─FAIL ─► 返回 [其他消费者正在处理]
    │
    ├─SUCCESS ──┐
    │           │
    │           ▼ (获得锁后)
    │     FOR(;;) ◄─────────────┐
    │      {                    │
    │        │                  │
    │        ├─ 重新查询实体    │
    │        │  ├─NOT FOUND     │
    │        │  │   └─► 返回    │
    │        │  │               │
    │        │  └─FOUND         │
    │        │      │           │
    │        │      ├─► 状态更新 INIT→PROCESSING
    │        │      │
    │        │      ├─► 构建领域模型
    │        │      │  ├─ DAGInstance
    │        │      │  ├─ TaskInstance
    │        │      │  └─ RuntimeContext
    │        │      │
    │        │      ├─► 检查是否需要停止(终态)
    │        │      │   ├─YES ─► 返回
    │        │      │   └─NO
    │        │      │
    │        │      ├─► 检查是否需要等待
    │        │      │   ├─YES ─► 发送延迟消息 + 返回
    │        │      │   └─NO
    │        │      │
    │        │      ├─► 调用 WorkFlowProcessor.pushOneStep()
    │        │      │   │
    │        │      │   ├─► LocalTaskProcessor.taskRun()
    │        │      │   │   │
    │        │      │   │   ├─► TaskRunnerFactory.getTaskRunner()
    │        │      │   │   │
    │        │      │   │   ├─► runner.process()
    │        │      │   │   │   └─► WorkFlowTaskResult
    │        │      │   │   │
    │        │      │   │   └─► 处理结果
    │        │      │   │       ├─ SUCCESS: onSuccessStatus()
    │        │      │   │       ├─ FAIL: onFailStatus()
    │        │      │   │       ├─ POLLING: handlerPolling()
    │        │      │   │       └─ ...
    │        │      │   │
    │        │      │   ├─► 构建新Entity
    │        │      │   │
    │        │      │   └─► 返回 WorkFlowTaskNodeStatusEnum
    │        │      │
    │        │      ├─► Thread.sleep(2000) [等待从库同步]
    │        │      │
    │        │      ├─► 检查返回状态
    │        │      │   ├─ LATER_CONTINUE_PROCESSING ─► BREAK
    │        │      │   ├─ WAITING_NODE_RESULT ─────► BREAK
    │        │      │   └─ 其他状态 ─────────────► 继续循环 ─┐
    │        │      │                                        │
    │        │      └─► [异常捕获]                           │
    │        │          ├─ 设置FAILURE状态                  │
    │        │          ├─ 执行失败回调                      │
    │        │          └─ 抛出异常                          │
    │        │                                               │
    │        └─► BREAK ◄─ 所有终止条件都会BREAK ───────────┘
    │      }
    │      [循环结束]
    │
    └─► 释放分布式锁
        │
        └─► 返回
```

### 6.2 任务执行决策树（完整版）

```
taskRun() 返回 WorkFlowTaskResult
    │
    ├─► status: SUCCESS
    │   │
    │   ├─► nextStepOnSuccess: null?
    │   │   ├─YES ─► FINISHED (工作流完成)
    │   │   │
    │   │   └─NO ──► stepName = nextStepOnSuccess
    │   │            CONTINUE_PROCESSING
    │   │
    │   └─► 返回 SUCCESS 到 pushOneStep
    │       └─► onSuccessStatus()
    │           ├─ 清除 taskRuntimeContext
    │           ├─ 更新 subStatus (状态机)
    │           ├─ 记录 digestLog
    │           └─ 如果是最后一步，执行成功回调
    │
    ├─► status: FAIL
    │   │
    │   └─► handleFail()
    │       │
    │       ├─► canReRun? (检查reRunStrategy)
    │       │   ├─YES ─► PROCESSING
    │       │   │        ├─ 取消任务
    │       │   │        ├─ 重置重试/轮询策略
    │       │   │        └─ 设置延迟 (如有)
    │       │   │
    │       │   └─NO ──► canRetry? (检查retryStrategy)
    │       │            ├─YES ─► NODE_RETRY_PROCESSING
    │       │            │        ├─ 减少重试计数
    │       │            │        └─ 设置延迟 (如有)
    │       │            │
    │       │            └─NO ──► FAIL (最终失败)
    │       │                     ├─ 取消任务
    │       │                     ├─ 执行失败回调
    │       │                     └─ 设置 FAILURE 状态
    │       │
    │       └─► 返回到 pushOneStep
    │           └─► onFailStatus()
    │               ├─ 设置 status = FAILURE
    │               ├─ 更新 subStatus
    │               └─ 执行失败回调
    │
    ├─► status: POLLING
    │   │
    │   └─► handlerPolling()
    │       │
    │       ├─► pollingStrategy: null 或 超时?
    │       │   ├─YES ─► FAIL (轮询超时)
    │       │   │
    │       │   └─NO ──► WAITING_NODE_RESULT
    │       │            ├─ 减少轮询计数
    │       │            └─ 设置 appointExecuteTimeMills
    │       │
    │       └─► 返回 WAITING_NODE_RESULT
    │           [下一轮消息触发重新执行]
    │
    ├─► status: CONTINUE
    │   │
    │   └─► CONTINUE_PROCESSING
    │       [立即继续下一个步骤]
    │
    ├─► status: LATER_CONTINUE
    │   │
    │   └─► LATER_CONTINUE_PROCESSING
    │       [发送延迟消息后继续]
    │
    ├─► status: QUEUE_PROCESSING
    │   │
    │   └─► QUEUE_PROCESSING
    │       [等待队列处理]
    │
    ├─► status: DOING_FAIL_DIAGNOSIS
    │   │
    │   └─► handlePollingDiagnose()
    │       └─► WAITING_FAIL_DIAGNOSIS_RESULT
    │           [轮询诊断结果]
    │
    ├─► status: FINISH_FAIL_DIAGNOSIS
    │   │
    │   └─► handleFinishDiagnose()
    │       └─► FINISH_FAIL_DIAGNOSIS
    │           [诊断完成]
    │
    └─► status: WORKFLOW_DISUSE
        │
        └─► WORKFLOW_DISUSE
            ├─ 取消任务
            ├─ 设置 status = DISUSE
            └─ 执行停用回调
```

---

## 7. 时间序列与触发流程

```
时间线 ═══════════════════════════════════════════════════════════════

T0: 实例创建
   │
   ├─► 写入数据库
   └─► 发送初始消息到Mafka
         │
         └─ [消息队列中待处理]

T1: 消息被消费
   │
   ├─► WorkFlowConsumerService 接收消息
   ├─► 查询最新实例
   ├─► WorkFlowService.process()
   ├─► 获取分布式锁 ✓
   │
   └─► 执行第一个步骤
       │
       └─► 返回状态

T1 + 返回状态决策:
   │
   ├─► 如果 SUCCESS 且有下一步
   │   └─► [返回，等待下一轮消息]
   │
   ├─► 如果需要轮询 (WAITING_NODE_RESULT)
   │   │
   │   └─► 计算 delaySeconds = (appointExecuteTime - now) / 1000
   │       │
   │       ├─ < 60s ─► 立即发送延迟消息
   │       └─ ≥ 60s ─► 发送 Mafka 延迟消息
   │
   ├─► 如果需要重试/重跑
   │   │
   │   └─► 设置 appointExecuteTime = now + intervalMills
   │       └─► 发送延迟消息
   │
   └─► 如果终态 (FINISHED/FAILURE/DISUSE)
       └─► 不再发送消息

T2: 延迟消息到期
   │
   └─► Mafka 重新投递消息到消费端
       │
       └─► 流程从 T1 重复 (第二轮处理)

T2': 状态检查
   │
   ├─► [获取分布式锁，若失败则返回]
   ├─► [检查状态是否已终态]
   ├─► [执行下一个步骤或继续轮询]
   │
   └─► ...

...

TN: 最终状态
   │
   ├─► 状态转移到 FINISHED/FAILURE/DISUSE
   ├─► 执行对应回调
   └─► 不再发送消息
```

---

## 总结

这些图表展示了：

1. **数据流向**: 从数据库持久化到具体执行
2. **状态机**: 实例级、子状态级、任务级三层状态转移
3. **序列流**: 消息消费到任务执行的完整序列
4. **决策点**: 重试/重跑/轮询的决策逻辑
5. **依赖关系**: 各实体之间的1:1、1:N关系
6. **处理循环**: 自旋锁中的完整处理流程
7. **时间触发**: 消息触发、延迟消息、轮询周期

这些图表可以帮助理解整个工作流的运行机制、各组件间的协作、以及数据在不同阶段的转换过程。

