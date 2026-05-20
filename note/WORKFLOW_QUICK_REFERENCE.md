# 工作流运行机制 - 快速参考指南

## 📋 核心实体速查表

### CustomWorkFlowInstanceEntity (工作流实例)
```
┌─────────────────────────────────────────────────────────┐
│ 数据库表：custom_work_flow_instance                      │
├─────────────────────────────────────────────────────────┤
│ 字段              │ 说明                  │ 更新时机       │
├──────────────────┼──────────────────────┼───────────────┤
│ id               │ 实例ID                │ 创建时         │
│ bizIdentity      │ 业务身份              │ 创建时         │
│ templateName     │ 模板名称              │ 创建时         │
│ templateVersion  │ 模板版本              │ 创建时         │
│ currentStepName  │ 当前步骤 ⭐           │ 每次推进       │
│ status           │ 实例状态 ⭐           │ 关键点         │
│ subStatus        │ 子状态                │ 每次推进       │
│ executeDag       │ DAG定义(JSON)        │ 创建时         │
│ taskNodeSnapshot │ 任务快照(JSON) ⭐    │ 中间状态       │
│ digestLog        │ 摘要日志              │ 每次推进       │
│ runtimeContext   │ 运行上下文(JSON)      │ 必要时         │
│ relatedId        │ 关联业务ID            │ 创建时         │
└─────────────────────────────────────────────────────────┘
```

### WorkFlowRuntimeContext (运行时上下文)
```
┌─────────────────────────────────────────────┐
│ WorkFlowRuntimeContext                      │
├─────────────────────────────────────────────┤
│ ┌─ TaskRuntimeContext ──────────────────┐  │
│ │ ├─ retryStrategy                      │  │
│ │ ├─ reRunStrategy                      │  │
│ │ ├─ pollingStrategy                    │  │
│ │ ├─ pollingDiagnosisModel              │  │
│ │ ├─ appointExecuteTimeMills            │  │
│ │ └─ startExecuteTimeMills              │  │
│ └───────────────────────────────────────┘  │
│                                             │
│ ┌─ ProcessInstanceRuntimeContext ───────┐  │
│ │ ├─ digestLog (摘要日志)               │  │
│ │ └─ runContext (业务运行时数据)        │  │
│ └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

---

## 🔄 状态转移速查

### 工作流实例状态
```
INIT ──[消息触发]──> PROCESSING ──┬──> FINISHED ✓
                                 ├──> FAILURE ✗
                                 └──> DISUSE ⊗
```

### 任务节点返回状态
```
SUCCESS                  ─────> 获取下一步
FAIL                     ─────> 检查重试/重跑
POLLING                  ─────> 等待结果
WAITING_NODE_RESULT      ─────> 等待下一轮消息
LATER_CONTINUE_PROCESSING ─────> 发延迟消息
WORKFLOW_DISUSE          ─────> 停用
```

---

## ⚙️ 处理流程速查

### 单次消息处理流程
```
1️⃣ WorkFlowConsumerService.receiveWorkFlowData()
   ├─ 验证非空
   ├─ 解析JSON
   ├─ 黑名单检查
   └─ 调用processWorkFlowMessage()

2️⃣ WorkFlowService.process()
   ├─ 状态检查
   ├─ 环境检查
   ├─ 获取分布式锁
   ├─ for(;;) 循环 {
   │   ├─ 重新查询实例
   │   ├─ 更新状态: INIT→PROCESSING
   │   ├─ 构建领域模型
   │   ├─ WorkFlowProcessor.pushOneStep()
   │   ├─ Thread.sleep(2000)
   │   └─ 检查返回状态
   │ }
   └─ 释放锁

3️⃣ WorkFlowProcessor.pushOneStep()
   ├─ LocalTaskProcessor.taskRun()
   ├─ 根据返回状态处理
   │  ├─ SUCCESS: onSuccessStatus()
   │  ├─ FAIL: onFailStatus()
   │  └─ ...
   ├─ 更新Entity
   └─ 保存数据库

4️⃣ LocalTaskProcessor.taskRun()
   ├─ 获取TaskRunner
   ├─ 检查停止标记
   ├─ taskRunner.process()
   ├─ 根据result.status分类
   │  ├─ SUCCESS: return SUCCESS
   │  ├─ FAIL: handlerFail() -> 重试/重跑
   │  ├─ POLLING: handlerPolling()
   │  └─ ...
   └─ 返回WorkFlowTaskNodeStatusEnum
```

---

## 🎯 重试/重跑/轮询决策树

### 失败处理决策 (handlerFail)
```
FAIL
 │
 ├─► 能重跑? (优先级1)
 │   ├─YES ─► PROCESSING
 │   │        • 取消任务
 │   │        • 重置所有策略
 │   │        • 设置延迟
 │   │
 │   └─NO ──────┐
 │              │
 └──────────────┤
 ├─► 能重试? (优先级2)
 │   ├─YES ─► NODE_RETRY_PROCESSING
 │   │        • 不取消任务
 │   │        • 设置延迟
 │   │
 │   └─NO ──────┐
 │              │
 └──────────────┤
 └─► FAIL (优先级3)
     • 取消任务
     • 设置失败状态
     • 执行失败回调
```

### 轮询处理决策 (handlerPolling)
```
POLLING
  │
  ├─► 超时? (currentTimes <= 0)
  │   ├─YES ─► FAIL
  │   │        • 取消任务
  │   │        • 返回失败
  │   │
  │   └─NO ──► WAITING_NODE_RESULT
  │            • 减少计数
  │            • 设置下次执行时间
  │            • 等待下一轮消息
```

---

## 📊 重要参数速查

### 重试参数示例
```
WorkFlowTaskRetryModel(3, 3, 3000)
├─ totalTimes: 3        // 总共3次
├─ currentTimes: 3      // 当前剩余3次（递减）
└─ intervalMills: 3000  // 间隔3秒
```

### 重跑参数示例
```
WorkFlowTaskReRunModel(3, 60000)
├─ totalTimes: 3        // 总共3次
├─ currentTimes: 3      // 当前剩余3次（递减）
└─ intervalMills: 60000 // 间隔60秒
```

### 轮询参数示例
```
WorkFlowTaskPollingModel(120, 60000)
├─ totalTimes: 120      // 总共轮询120次
├─ currentTimes: 120    // 当前剩余120次（递减）
└─ intervalMills: 60000 // 间隔60秒 = 最多2小时
```

---

## 🔑 关键方法速查

### WorkFlowConsumerService
```
receiveWorkFlowData(String)          // 消息入口
  └─► processWorkFlowMessage(WorkFlowMessage)
```

### WorkFlowService
```
process(CustomWorkFlowInstanceEntity)
  ├─ needStop()                      // 检查是否终态
  └─ needWait()                      // 检查是否需要延迟
```

### WorkFlowProcessor
```
pushOneStep(...)                     // 推进一步
  ├─ taskProcess()                   // 执行任务
  ├─ onSuccessStatus()               // 成功处理
  ├─ onFailStatus()                  // 失败处理
  └─ onWorkFlowDisuseStatus()        // 停用处理
```

### LocalTaskProcessor
```
action(...)                          // 执行动作
  └─ taskRun()
     ├─ handlerFail()                // 重试/重跑判断
     ├─ handlerPolling()             // 轮询判断
     └─ handlerContinue()            // 继续判断
```

---

## 🚀 常见场景速查

### 场景1: 任务成功，有下一步
```
状态: SUCCESS
操作:
  1. 获取 nextStepOnSuccess
  2. 更新 stepName = nextStepName
  3. 继续循环 (自旋处理下一步)
结果: 下一个步骤在同一消息中执行
```

### 场景2: 任务失败，能重试
```
状态: FAIL (canRetry = true)
操作:
  1. 减少 retryStrategy.currentTimes
  2. 设置 appointExecuteTimeMills = now + interval
  3. 返回 NODE_RETRY_PROCESSING
结果: 发送延迟消息，下一轮重试
```

### 场景3: 任务轮询，未完成
```
状态: POLLING (not timeout)
操作:
  1. 减少 pollingStrategy.currentTimes
  2. 设置 appointExecuteTimeMills = now + interval
  3. 返回 WAITING_NODE_RESULT
结果: 发送延迟消息，下一轮继续轮询
```

### 场景4: 任务轮询超时
```
状态: POLLING (timeout)
操作:
  1. 取消任务
  2. 记录超时日志
  3. 返回 FAIL
结果: 进入失败处理流程 (重试/重跑/最终失败)
```

### 场景5: 工作流完成
```
状态: SUCCESS && nextStepOnSuccess == null
操作:
  1. 设置状态 FINISHED
  2. 执行成功回调
  3. 返回 (不再发送消息)
结果: 工作流完成，实例置为终态
```

---

## 💾 数据序列化/反序列化速查

### executeDag 反序列化
```java
WorkFlowDagTemplateDefine template =
  Obj.fromJsonString(entity.getExecuteDag(), WorkFlowDagTemplateDefine.class);
```

### taskNodeSnapshot 反序列化
```java
TaskRuntimeContext taskContext =
  Obj.fromJsonString(entity.getTaskNodeSnapshot(), TaskRuntimeContext.class);
```

### runtimeContext 反序列化
```java
Map<String, Object> runtimeContext =
  Obj.fromJsonString(entity.getRuntimeContext(), Map.class);
```

---

## 🔍 调试要点速查

### 如何追踪工作流执行
```
1. 查看数据库表 custom_work_flow_instance:
   ├─ status: 当前状态 (INIT/PROCESSING/FINISHED/FAILURE/DISUSE)
   ├─ currentStepName: 当前步骤
   ├─ digestLog: 执行日志
   └─ taskNodeSnapshot: 重试/轮询状态

2. 查看日志:
   ├─ WorkFlowConsumerService: 消息接收日志
   ├─ WorkFlowService: 处理流程日志
   ├─ WorkFlowProcessor: 推进日志
   └─ LocalTaskProcessor: 任务执行日志

3. CAT监控:
   ├─ WorkFlowConsumer/process: 消费耗时
   ├─ workflow.node.execute.time: 节点执行耗时
   └─ WorkFlowTaskRunnerErr: 错误统计
```

### 如何判断工作流卡住
```
1. 查看 currentStepName
   └─► 检查该步骤的 taskNodeSnapshot
       ├─ appointExecuteTimeMills 是否已过期?
       ├─ pollingStrategy.currentTimes 是否耗尽?
       └─ 是否被分布式锁阻塞?

2. 查看 digestLog
   └─► 最后一个操作是什么?
       ├─ "xxx:success" → 成功
       ├─ "xxx:fail" → 失败
       ├─ "xxx:轮询策略结束,超时未完成" → 轮询超时
       └─ "process error:..." → 异常

3. 查看最后更新时间
   └─► 计算 (now - updateTime)
       ├─ < 1分钟: 可能刚处理完，等待下一轮消息
       ├─ > 1小时: 可能卡住了，检查分布式锁
       └─ > 24小时: 检查是否被遗忘
```

### 如何强制停止工作流
```
修改数据库:
  UPDATE custom_work_flow_instance
  SET status = 'DISUSE',
      sub_status = 'FORCED_FAIL',
      digest_log = concat(digest_log, '; forced stop')
  WHERE id = ?

然后:
  发送一条消息到Mafka，触发处理
  └─► WorkFlowService 会检查状态，执行停用回调
```

---

## 📈 性能优化建议

### 延迟消息策略
```
├─ < 60秒: 直接调用 delaySendWorkFlowMessage()
│          └─ 避免 Mafka 延迟消息的成本
│
└─ >= 60秒: 发送到 Mafka 延迟消息队列
           └─ 减少内存占用
```

### 轮询策略建议
```
快速轮询:    pollingModel = new WorkFlowTaskPollingModel(30, 10000)
            ├─ 总共轮询30次，每次10秒 = 5分钟

中速轮询:    pollingModel = new WorkFlowTaskPollingModel(120, 60000)
            ├─ 总共轮询120次，每次60秒 = 2小时

慢速轮询:    pollingModel = new WorkFlowTaskPollingModel(1440, 60000)
            ├─ 总共轮询1440次，每次60秒 = 1天
```

### 重试策略建议
```
aggressive:  retryModel = new WorkFlowTaskRetryModel(5, 1000)
             ├─ 快速重试，适合网络瞬时故障

moderate:    retryModel = new WorkFlowTaskRetryModel(3, 5000)
             ├─ 中等重试，适合大多数场景

conservative: retryModel = new WorkFlowTaskRetryModel(2, 10000)
              ├─ 保守重试，适合稳定的系统
```

---

## 📞 关键类速查表

| 类名 | 模块 | 职责 |
|------|------|------|
| WorkFlowConsumerService | MIS | Mafka消息消费 |
| WorkFlowService | Core | 工作流编排 |
| WorkFlowProcessor | Core | 单步推进 |
| LocalTaskProcessor | Core | 任务执行 |
| ITaskRunner | Core | 具体业务逻辑 |
| WorkFlowModelHelper | Core | 模型构建 |
| WorkFlowCallbackManager | Core | 回调管理 |
| WorkFlowMessageSender | Core | 消息发送 |
| RedisLockGateway | Common | 分布式锁 |
| WorkFlowModelService | DAL | 数据访问 |

---

## ⚡ 关键信息总结

### 三层结构
```
消费端 (MIS) ──► 编排层 (Core) ──► 执行层 (Runner)
```

### 二大核心机制
```
1. 分布式锁    ──► 防止并发处理
2. 自旋循环    ──► 单消息多步骤处理
```

### 三层策略
```
1. 重跑 (ReRun)      ──► 优先级最高，取消并重新执行
2. 重试 (Retry)      ──► 优先级次高，同步重试
3. 轮询 (Polling)    ──► 优先级最低，等待异步结果
```

### 四个关键时间点
```
1. INIT → PROCESSING      ──► 处理开始
2. PROCESSING → 终态      ──► 处理结束
3. appointExecuteTime     ──► 下次执行时间
4. Thread.sleep(2000)     ──► 主从同步等待
```

### 五个终态
```
1. FINISHED         ──► 成功完成
2. FAILURE          ──► 失败
3. DISUSE           ──► 主动停用
4. FORCED_FAIL      ──► 强制失败
5. (sub_status)     ──► 细粒度状态

