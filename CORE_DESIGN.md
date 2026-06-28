# coreflow 核心内核标准

## 标准

### 1. 设计原则摘要

本标准从零开始定义 `coreflow`。它是一个最小 AI workflow kernel，公共入口是单一序列化优先的 `Graph`；单一职责函数叫 `plug`，plug 之间的依赖和字段流向叫 `flow`。

这套设计受 Unix、微内核、Runner job lifecycle 和序列化数据模型约束驱动。参考材料只提供原则来源；本标准的具体定义以本节后续领域语言、Flow、Kernel 和存储协议定义为准。

本标准采用一个单一的 `Graph` 公共模型，并把它分成 Kernel、Graph、GraphStore 与 GraphResult 四层来理解。

本节只固定阅读顺序：先划行为边界，再谈文件和模块；先确定 plug、flow、Graph、GraphStore、GraphResult 的语义，再进入实现结构。

### 2. 领域语言

为了保持概念稳定，领域语言需要按层理解。本节按依赖关系排序：先定义核心对象与协议边界，再定义 graph 存储、flow planning、kernel execution，最后定义输出和扩展能力。

#### 2.1 核心对象与协议边界层

**Plug**：一个单一职责函数。它是 workflow 的最小业务行为单位，通过可序列化调用边界进入 Graph。

**Flow**：plug 之间的依赖、反馈和数据流关系。Flow 描述谁监听谁、运行时如何构建下游 PlugInput，以及哪些输入变化会产生 tick。Graph 承载图结构和运行入口，plug 承载业务策略，policy 只承载调度策略。

**Graph**：workflow 的公共图模型。Graph 保存 plug 注册表、flow 关系和运行配置。用户创建 graph，注册 plug 实现，加入 graph-local plug，删除 plug，加入依赖/字段流向，删除依赖/字段流向。

**Value**：跨 plug 的运行时数据。Value 表示可序列化的结构化数据，是 typed pipe 的载荷。

#### 2.2 Graph 存储描述层

**GraphStore**：graph 文件的版本化存储。它保存当前完整 Graph、head commit 和更改记录。

**GraphCommit**：一次 graph 更改。它记录 parent、message 和 GraphChange。

**Graph**：某个 commit 下的完整 graph 描述。它保存 plug kind 到 plug name 的映射，以及 target-keyed flow。

**Plug**：graph 文件中的 plug 名。Graph 文件按 plug kind 分组保存 plug name；plug name 用于 flow 的 source 和 target。同一个 Graph 内 plug name 必须唯一，即使对应的 plug kind 不同也不能重名。

#### 2.3 Flow planning 与 PlugInput 构建层

**Flow**：graph 文件中的依赖关系与输入来源表。它以 target plug 为第一层 key，描述 target plug 依赖哪些 source plug，以及 target input field 从哪个 source output path 读取值。

**InputBind**：由 Graph、Plug 和用户的 JSON flow 声明生成的输入绑定计划。它描述 target input field 应从哪个 source output path 读取值。

**PlugInput**：某次 tick 真正喂给目标 plug 的输入 Value。它由当前运行结果和 InputBind 构建出来，再进入 plug 的输入边界。

**FieldPath / Selector**：指向 Value 内部字段的声明式路径。它只表达 object field、array index 和缺失路径错误。

**PlugInput 构建**：plug 收到 tick 前，根据 InputBind、SourceSelector 和 FieldPath，把 source output path 写入 target input field，并生成 PlugInput 的过程。它只读取路径、写入路径和报告结构化错误。

**SourceSelector**：Flow 内的字段来源 selector。它表达 source plug 和 source output path。

#### 2.4 Graph 运行事实层

**GraphResult**：某次运行的输出与运行事实。它不写入 Graph 文件。

#### 2.5 Check 与 kernel 执行层

**Graph Check**：Graph 运行前的声明检查流程。Check 验证 plug、flow 和 selector 的静态一致性，并刷新 Graph 内部执行缓存。

**Execution Caches**：Graph 内部为了运行效率维护的执行缓存，例如 plug name 到 `PlugId` 的映射、节点级反向依赖缓存、InputBind 缓存和 Plug hash。它们作为 Graph 内部 cache 存在；Graph 修改后可以失效并重建。

**Kernel**：执行 Graph 当前 checked snapshot 的最小调度机制。Kernel 负责 tick detection、concurrency、flow bookkeeping 和 result table。字段绑定归属 PlugInput 构建，调度策略归属 ExecutionPolicy；timeout 和 cancel 由普通 plug 产出的 Value 表达。

**ExecutionPolicy**：一次 graph run 的调度策略输入。它只表达 kernel 如何调度已满足的 tick，例如失败策略和并发上限；Picker 选择作为一次运行的公开调度参数传入 kernel，保持调度配置的公开形态稳定。ExecutionPolicy 不表达业务路由、timeout、cancel、人工审批或继续/结束决策。ExecutionPolicy 由运行入口或宿主环境提供，不写入 Graph 文件。

**Tick**：一次 plug 运行请求。Tick 说明哪个 plug 因为什么输入变化、seed、manual trigger 或恢复状态而需要运行。

**Tick Queue**：等待 Runner 启动的 tick 队列。它保存待执行工作；graph 可以包含反馈环。

**Picker**：从 Tick Queue 中选择下一批 tick 的调度策略。它属于一次运行的公开调度选择，只表达 ready tick 的选择顺序，不表达业务路由。

**Runner**：持有 jobs 并驱动一次 graph run 的执行者。Runner 从 Tick Queue 取 tick，启动 job，收割完成结果，传播 flow，并判断 Idle 或 failure。

**Job**：Runner 已经启动、尚未被收割的 plug 运行任务。Job 必须由 Runner 持有，并由 Runner 统一收割。

**Idle**：一次 graph run 的稳定终态。Idle 表示 Tick Queue 为空、jobs 为空、输入变化已经传播完毕。Kernel 对线性 flow 和反馈 flow 使用同一套 Idle 判断。

**Feedback Flow**：flow 边组成反馈环时形成的普通 graph 形态。用户用普通 plug 表达继续、结束、超时和取消决策。

#### 2.6 输出、报告与扩展能力层

**GraphOutput**：一次 graph run 的用户可读取结果视图。它是 `GraphResult.outputs` 的读取层，按 plug name 读取最新稳定输出，不把并发完成顺序暴露为业务语义。

**GraphResult events**：一次 graph run 的可观察事件。事件包含 graph started、tick queued、plug input built、job started、job done、job failed、flow propagated、graph idle、duration 和 tick wait time。

### 3. 关键概念分析

#### 3.1 Plug 定义

Plug 必须保持 Unix 式单一职责。一个 plug 的接口应该尽量像函数：一个输入，一个输出，一个错误通道。多个返回值通过一个具名结构体表达。这样上游输出字段有稳定名字，下游 flow 也可以引用名字。

Plug 的协议边界是：接收一个符合输入 schema 的 Value，执行单一职责行为，返回一个符合输出 schema 的 Value 或结构化错误。Graph 执行固定流程：构建运行时 Value，调用 plug，再把 plug 输出放回运行时 Value。这是本标准保证的 plug 边界。

但是 plug 还需要补充几个运行时属性。第一，是否可重入：如果 plug 内部持有可变状态，同一个 plug 是否允许并发多次运行。

反馈 flow 允许同一 plug 在单次 graph run 中被多次 tick 触发，因此必须知道 plug 是否可复制执行、是否可并发共享、是否只能串行。

第二，纯函数只是可选属性。AI workflow 里很多 plug 会访问网络或工具，kernel 按 Plug 调度声明处理这些属性，ExecutionPolicy 只处理调度选择。

第三，plug 返回业务输出；暂停、人工审批、timeout 和 cancel 建模为普通 plug 输出。

#### 3.2 Flow 定义

Flow 的职责是表达依赖关系和数据流关系。依赖关系回答“B 是否必须等待 A 完成”。数据流关系回答“A 的哪些输出会参与 B 的输入”。如果 B 的 flow 声明来自 A，则 A 的输出参与 B 的 PlugInput 构建；精确 InputBind 负责表达字段级绑定，同时保持依赖显式。

flow 允许有环。AI workflow 很可能出现反馈：一个 plug 的输出会改变上游上下文、重写计划、触发重新评分，或者把人工反馈送回前面的步骤。反馈环是普通 graph 形态；Kernel 必须把 flow 看成可传播的 value graph。

反馈 flow 只有一条 kernel 规则：输入 snapshot 发生变化才给目标 plug 发 tick；输出保持稳定时传播完成。开始、继续、结束、timeout 和 cancel 都是业务决策，用户通过普通 plug 输出的 Value 表达。Kernel 传播 Value，并在 tick queue 与 jobs 清空时进入 Idle。

Flow 通过 JSON 声明上游和下游关系，并用少量 declarative selector 表达字段选择。字段选择属于 flow；业务转换属于 plug。

#### 3.3 Graph 定义

Graph 的职责是保存 plug 和 flow。它负责把两者收进同一个 workflow surface，并把运行配置和输出读取入口暴露给用户。

Graph 中的 flow 允许有环。feedback graph 不是例外形态，而是普通 graph 形态。Graph 需要把这种形态交给 Kernel 执行，并保持结果可解释。

Graph 把业务分支逻辑放在 plug 输出的结构化 decision 中。policy 只影响调度选择；kernel 聚焦依赖、调度、并发和可解释结果。

### 4. Plug 声明、Graph 存储与 Value 协议层

Plug 声明是注册期事实。实现层可以生成或手写 `Plug` 签名和可序列化调用边界；注册流程把这些声明加入当前运行环境。

运行时只消费已经注册的 Plug 声明、Graph 中的 flow、运行时 Value 和 check 后的 InputBind。字段发现和字段校验基于声明数据完成；kernel 的事实源保持为 Graph、Plug registry、Value 和 GraphResult。

本层先定义 graph 文件如何保存声明，再定义运行时 value 如何穿过 plug 边界。存储协议和 Value 协议为 flow、check 和 run 提供共同基础。

#### 4.1 Graph 文件存储模型

Graph 文件存储围绕 `GraphStore` 展开。领域对象定义见 `标准 / 2.2 Graph 存储描述层`；本节只规定磁盘形态和读写语义。

`Flow` 是整张 graph 的依赖关系与输入字段来源表。第一层 key 是目标 plug 名；`InputMap` 是某个 plug 的输入字段来源表；第二层 key 是目标 input field；`SourceSelector` 表达来源 plug 和来源字段路径。JSON 存储仍然保持 `{ "B": { "recipient": "A.profile.email" } }` 的形状。

Graph 文件保存 plug kind 到 plug name 的映射，以及 flow 结构。Plug 签名、plug 实现和 ExecutionPolicy 由宿主环境或 Graph 运行入口提供，不写入 graph 文件。

`GraphStore.graph` 保存当前完整 `Graph`。`GraphCommit` 保存一次更改，而不是完整 graph snapshot。磁盘形态是单一 `graph.json`，同时保存 `head`、当前完整 `Graph` 和全部 `GraphCommit` 更改记录。读取当前 graph 只读取这个文件中的当前 graph；需要审计或回放时，再按 commit 链应用 `GraphChange`。

GraphStore 支持导出和导入。导入只恢复 graph 文件存储；执行能力来自宿主环境中已注册的 plug implementation。

GraphStore 同时保留历史提交记录和最后一次完整图数据。`graph` 字段是当前 head 下的完整 Graph；`commits` 字段是审计、解释和可选回放用的历史链。普通加载当前 graph 时读取 `graph`，不要求回放提交链；需要审计或重建历史状态时，才按 `head` 和 `commits` 回放。

`graph.json` 内容是一个完整 `GraphStore`：

```json
{
  "head": "01J...",
  "graph": {
    "plugs": {
      "draft_step": ["draft"],
      "review_step": ["review"]
    },
    "flow": {
      "draft": {
        "feedback": "review.comment"
      }
    }
  },
  "commits": {
    "01J...": {
      "id": "01J...",
      "parent": "01H...",
      "message": "add review feedback flow",
      "change": {
        "flow_in": {
          "target": "draft",
          "input": "feedback",
          "source": "review.comment"
        }
      }
    }
  }
}
```

Graph 文件存储与 graph 执行分开。GraphResult 是运行结果，不存进 Graph 文件。`flow` 本身是以 target plug 为 key 的依赖关系与输入来源映射；`check` 可以从它生成节点级反向依赖缓存，让 source plug 完成时快速找到可能被唤醒的 target plug。字段级来源仍只保存在 `flow` 中，由 target plug 构建 PlugInput 时读取。

运行结果和运行时状态分开保存。GraphResult 记录某次 run 的输出、事件和状态。RunState 只存在于一次 run 内部。

`Graph` 构建环节可以收集错误；`commit` 环节写入新的 GraphCommit；`check` 环节检查未知 plug、flow path、Plug 和启动条件，并刷新执行缓存；`run` 环节必须自动执行 check 并消费 checked Graph。反馈环不在 check 环节被拒绝；run 环节只用 tick、change detection 和 Idle 处理传播，并产出 GraphResult。

`GraphChange::Replace` 表达“用一个完整 Graph 替换当前 Graph”的提交语义。它服务于自适应 plug 产出 next Graph 的场景：GraphStore 仍保存最后一次完整 `graph`，同时把替换动作作为 GraphCommit 留在历史链中。回放提交链时，`Replace` 直接把当前 replay graph 置为提交中的完整 graph；之后的 `PlugIn`、`PlugOut`、`FlowIn`、`FlowOut` 再按顺序增量应用。

ExecutionPolicy 属于 run 的调度输入，而不是 graph 声明。相同 GraphStore 可以用不同 ExecutionPolicy 运行，例如本地调试时使用单 worker，生产运行时使用更高 max_concurrency。改变 ExecutionPolicy 不产生 GraphCommit。

#### 4.2 Value 模型

Value 是 plug 之间传递的 JSON 结构化数据。Graph 只按 flow 读取字段、写入字段、比较 input snapshot；业务字段的含义归属 plug。

Value protocol 需要覆盖 object、array、string、number、boolean 和 null。字段路径只在 object 和 array 上移动。缺失字段、类型冲突和写入冲突都必须产生结构化错误。

示例：

```json
{
  "profile": {
    "email": "ada@example.com",
    "name": "Ada"
  },
  "score": 0.98,
  "tags": ["new", "verified"]
}
```

这个 Value 允许 flow 读取 `profile.email`、`profile.name`、`score` 或 `tags.0`。

Value 模型承载可序列化数据。plug 可以返回资源事实的可序列化描述；资源本体由 plug 管理。

### 5. Flow 与执行基础层

本层定义 flow 如何表达依赖、字段流向和 PlugInput 构建，再把这些声明接到 kernel 的执行规则上。

#### 5.1 Flow 的本质

用户提出的关键问题是：如果 A plug 返回一个包含多个具名字段的结构体，如何把 A 的某个具名返回值传给 B 的某个参数。这里有三个层次。

第一层是字段发现。系统需要知道 A 的输出有哪些字段，B 的输入需要哪些字段。这由 Plug 的签名提供，kernel 消费已确定的声明。

第二层是字段匹配。最简单情况是同名匹配：A 输出 `{ "email": "a@b" }`，B 输入 `{ "email": string }`。如果字段名不同，可以使用字段重命名规则、DTO plug，或者 declarative flow selector。

第三层是值转换。字段选择可以作为 flow selector；业务转换由 plug 表达。

因此，本标准要求 InputBind 使用 declarative selector。下面是 Graph check 后得到的规范化 InputBind 形态：

```json
{
  "target": "send_email",
  "input": {
    "recipient": { "from": "extract_user", "path": "profile.email" },
    "display_name": { "from": "extract_user", "path": "profile.name" },
    "subject": { "from": "draft_subject", "path": "subject" }
  }
}
```

这个 JSON 是 Graph check 生成的运行时数据。用户仍通过 target-keyed JSON flow 声明表达 flow。kernel 只拿到 `InputBind`，在 plug 收到 tick 时根据 input snapshot 生成 PlugInput。这样可以精确表达 flow，同时保持 plug 和 flow 的分层。

##### 5.1.1 Flow 存储

Flow 是 graph 文件的存储重点。Flow 回答 target plug 依赖哪些 source plug，以及每个输入字段来自哪个 source output path。source 输出让 target input snapshot 发生有效变化时，target 收到 tick。

#### 5.2 默认绑定策略

默认策略仍然保留，因为它让简单 graph 极其便宜。默认规则是：由 initial input 或 seed 产生 tick 的入口 plug 接收初始输入；单来源输入直接把上游输出作为下游输入；多来源输入组装成以来源 plug 名为 key 的 object。这个基础规则覆盖常见线性和 fan-in 场景：用户声明依赖 flow；未提供字段 selector 时，由 plug 输入边界负责字段匹配。

默认 InputBind 适合三类场景。第一，线性管道：A 输出结构刚好就是 B 输入结构。第二，简单字段重命名：字段重命名规则或 alias 足够解决。第三，多依赖 fan-in：下游输入字段名就是依赖 plug 名。

显式 flow InputBind 适合两类场景。第一，字段路径选择：从 `profile.email` 取到 `recipient`。第二，多个上游都有同名字段，需要消除歧义。它们都是 `Graph` 内的 PlugInput 构建策略，共享同一个 check 和运行模型。

#### 5.3 Kernel 并发模型

核心形态是 tick-driven 数据流传播。正确 kernel 维护 flow、result table、input snapshot table、version table、节点级反向依赖缓存和 tick queue。

初始 tick 来自 initial input、显式 seed、manual trigger 或上一次 run 的恢复状态。每当 plug 完成，kernel 把输出写入 `results[source]`，再用反向依赖缓存找到可能受影响的 target plug。

target plug 按自己的 `InputBind` 重新构建 PlugInput；如果 snapshot 发生有效变化，就把 target tick 推入队列。

Runner 持有正在执行的 job，并统一收割结果。每个 plug 完成后，Runner 更新结果版本和 tick queue。快完成的 plug 可以释放下游；慢 plug 不阻塞无关分支。

这比简单“按层并发”更适合 AI workflow。假设 A、B、C 同时收到 tick，D 监听 A，E 监听 B 和 C。如果 A 很快产出新结果，D 可以立即收到 tick，B 和 C 继续运行。如果 D 又反馈到 A，只要 A 的 input snapshot 确实变化，A 会重新入队；input snapshot 保持稳定时传播停止。这是数据流传播调度。

并发模型还要定义失败策略。默认采用 `FailFast`：任何 job 失败后，kernel 停止调度新 tick，等待已启动 jobs 完成并记录结果，报告失败 plug、tick_id 和已完成结果。`ContinueIndependent` 允许不依赖失败 tick 的分支保留执行资格。这个策略归属 `ExecutionPolicy`。

#### 5.4 调度策略与 policy seam

kernel 必须有一个默认调度策略。最小策略是 FIFO tick queue、change detection 和 `max_concurrency`。这足以让线性 flow、fan-in/fan-out 和反馈 flow 共享同一传播模型。策略集合包括 priority、成本估计、资源标签、rate limit、模型供应商限制和 GPU/CPU 区分。

调度策略通过 policy seam 注入。kernel 只消费调度策略结果；业务路由和业务决策归属 plug 输出。

内部结构写成小模块，默认 FIFO + 去重。用户只配置一次运行的 picker 选择；具体队列结构和选择器实现归属实现层，以同时保留调度可配置性和简洁 Graph surface。

#### 5.5 自组织、自调整、自适应的正确位置

用户希望 workflow 具备自组织、自调整、自适应能力。kernel 执行当前 checked Graph；负责修改 graph 的普通 plug 读取 Graph、GraphResult、资源状态和用户目标，产出新的 Graph，Graph check 后写入新的 GraphCommit。

这种 plug 仍是普通 plug：输入是可序列化的 graph 事实和运行事实，输出是可序列化的 graph 修改请求或 next Graph。Graph 负责应用修改、check 和 commit。

自适应模型有三步。第一，宿主把当前 GraphStore、上一次 GraphResult、资源状态和用户目标编码进普通 initial input、seed input 或上游 plug 输出；coreflow 不隐式注入这些事实。第二，普通 plug 根据这些可序列化事实生成 graph 修改请求或 next Graph。第三，可变 Graph 运行入口应用 graph 修改请求，执行 check，并写入新的 GraphCommit；kernel 执行新的 checked Graph。这样系统可以自适应，但每一次执行都绑定一个明确 GraphCommit id。

运行中的 kernel 保持当前 checked graph 不变。某个下游为什么运行了，某个 plug 为什么被跳过，某个字段为什么流向另一个字段，都可以从 Graph 和 GraphResult 解释。新的 graph 结构以 GraphCommit 保存；默认 `run` 入口是可变运行入口，调用者持有的 Graph head 会推进，下一次 run 使用新的 head。

默认可变 `run` 入口用输出 shape 识别自适应修改：一次 run 到达 Idle 后，Graph 遍历 plug 输出；如果某个输出可以反序列化为 `GraphMutationRequest`，就按其中的 `GraphChange` 应用并提交；如果某个输出可以反序列化为完整 `Graph`，就按 `GraphChange::Replace` 替换当前 Graph 并提交。识别出的自适应输出不会作为下一轮 initial input 继续传播，以避免同一个修改请求重复执行。业务 plug 如果可能输出与 `GraphMutationRequest` 或 `Graph` 同形的 JSON，应通过 schema、plug kind 或宿主约定避免误触发。

#### 5.6 Plug 声明与 flow 分层

Plug 声明分成四个层次。

第一层是注册期声明生成。实现层可以从类型、声明生成 Plug，也可以手写 Plug 声明。

第二层是 Plug registry。它保存当前进程可用的 Plug 声明和实现，检查名字冲突和版本冲突。plug 运行归属 kernel run 阶段。

第三层是 flow planner。它读取 flow 依赖和用户 selector，生成 InputBind。它负责提前发现缺字段和歧义字段冲突。

第四层是运行时 PlugInput 构建。它读取实际 output value，按 InputBind 组装 PlugInput。类型推断归属 check/planning；业务对象访问归属 plug implementation。

这个分层满足低样板使用需求：用户可以写很少代码，因为注册便利层可以生成 Plug。GraphStore/Plug/Value 协议保持可复用，运行时事实源保持单一。

#### 5.7 精确字段 Flow 设计

精确字段 Flow 通过 target-keyed JSON flow 声明表达；check 和 run 的语义由 `标准 / 5.1 Flow 的本质`、`标准 / 5.2 默认绑定策略` 和 `标准 / 5.3 Kernel 并发模型` 共同定义。

## 实现

### 1. Rust 类型定义

本节收纳标准部分拆出的 Rust 代码。标准部分只保留语义和数据形态。

```rust
pub struct GraphStore {
    pub head: CommitId,
    pub graph: Graph,
    pub commits: BTreeMap<CommitId, GraphCommit>,
}

pub struct GraphCommit {
    pub id: CommitId,
    pub parent: Option<CommitId>,
    pub message: String,
    pub change: GraphChange,
}

pub enum GraphChange {
    PlugIn { kind: PlugKind, name: PlugName },
    PlugOut(PlugName),
    FlowIn { target: PlugName, input: FieldPath, source: SourceSelector },
    FlowOut { target: PlugName, input: FieldPath },
    Replace { graph: Box<Graph> },
}

pub struct Graph {
    pub plugs: BTreeMap<PlugKind, Vec<PlugName>>,
    pub flow: Flow,
}

pub struct PlugKind(String);

pub struct PlugName(String);

pub struct Plug {
    pub name: PlugName,
}

#[serde(transparent)]
pub struct Flow(BTreeMap<PlugName, InputMap>);

#[serde(transparent)]
pub struct InputMap(BTreeMap<FieldPath, SourceSelector>);

pub struct SourceSelector {
    pub plug: PlugName,
    pub path: FieldPath,
}
```

```rust
pub struct GraphResult {
    pub graph_commit: CommitId,
    pub outputs: BTreeMap<PlugName, Value>,
    pub events: Vec<RunEvent>,
    pub status: GraphRunStatus,
}

pub enum RunEvent {
    GraphStarted,
    TickQueued { plug: PlugName, tick: u64 },
    PlugInputBuilt { plug: PlugName, tick: u64 },
    JobStarted { plug: PlugName, tick: u64 },
    JobDone { plug: PlugName, tick: u64 },
    JobFailed { plug: PlugName, tick: u64, error: CoreError },
    FlowPropagated { source: PlugName, target: PlugName },
    PendingApproval { plug: PlugName, tick: u64, reason: Option<String> },
    Duration { micros: u128 },
    TickWaitTime { plug: PlugName, tick: u64, micros: u128 },
    GraphIdle,
}

pub struct RunState {
    input_snapshots: Vec<Option<Value>>,
    versions: Vec<u64>,
    tick_queue: VecDeque<Tick>,
    jobs: JoinSet<JobOutcome>,
    results: Vec<Option<Value>>,
    failures: Vec<PlugFailure>,
}
```

```rust
pub struct ExecutionPolicy {
    pub failure: FailurePolicy,
    pub max_concurrency: usize,
    pub resource_limits: BTreeMap<String, usize>,
}
```

```rust
pub trait ValueCodec {
    type Value;
    fn encode<T: Serialize>(&self, value: T) -> Result<Self::Value, CoreError>;
    fn decode<T: DeserializeOwned>(&self, value: Self::Value) -> Result<T, CoreError>;
}
```

Rust 实现默认可以用 `serde_json::Value` 承载 Value protocol。`serde_json::Value` 和 Rust serde 直接集成，适合作为首个 wire value。Arrow、Protobuf 或自定义 core value model 可以通过 `ValueCodec` seam 接入；这些选择属于实现层。

```rust
pub trait Picker {
    fn pick(&mut self, queue: &mut TickQueue, capacity: usize) -> Vec<Tick>;
    fn on_done(&mut self, outcome: &JobOutcome);
}
```

```rust
pub struct FieldFlow {
    pub target_field: FieldPath,
    pub source_plug: PlugName,
    pub source_path: FieldPath,
    pub required: bool,
}
```

### 2. API 与模块结构层

本层把 public API、模块边界和实现结构收在一起。你先看 Graph surface，再看模块怎么分层，最后看各个子模块各自承担什么职责。

#### 2.1 API 标准：极简 serde Graph

核心 API 只保留极简 serde Graph。用户从 `Graph::new().plugup().plugin().flowin().run()` 理解系统：`plugup` 注册 Rust serde plug 实现，`plugin` 把已注册 kind 加入为 graph-local plug，`flowin` 用 JSON 声明依赖关系和可选字段来源，`run` 执行当前 graph 并返回 GraphResult。

API 采用五个 Graph 动词：`plugup`、`plugin`、`plugout`、`flowin`、`flowout`。`plugup(kind, func)` 把单一职责函数注册为某个 plug kind；`plugin(name, kind)` 把已注册 kind 的一个 graph-local plug 加入 Graph；`plugout` 从 Graph 删除 plug，并拒绝删除仍被 flow 引用的 plug；`flowin` 通过 JSON 加入依赖关系和可选字段来源；`flowout` 通过 JSON 删除字段来源或整条依赖 flow。

依赖 flow 和字段来源通过 `flowin(json!(...))` 入口表达：

```rust
let mut graph = Graph::new();

graph
    .plugup("coreflow.extract_user.v1", extract_user)?
    .plugup("coreflow.send_email.v1", send_email)?
    .plugin("extract_user", "coreflow.extract_user.v1")?
    .plugin("send_email", "coreflow.send_email.v1")?
    .flowin(json!({
        "send_email": {
            "recipient": "extract_user.profile.email",
            "display_name": "extract_user.profile.name"
        }
    }))?;
```

删除 API：

```rust
graph
    .flowout(json!({
        "send_email": ["recipient", "display_name"]
    }))
    .plugout("extract_user");
```

`flowout` 删除字段来源或整条依赖 flow；`plugout` 删除 plug，并且必须拒绝仍引用该 plug 的 flow。删除 plug 前，用户先 `flowout` 再 `plugout`，以保持 flow 引用明确。

GraphStore API：

```rust
let store = graph.store()?;
let json = serde_json::to_string_pretty(&store)?;
```

GraphResult 与自适应 graph 修改的语义见 `标准 / 4.1 Graph 文件存储模型` 和 `标准 / 5.5 自组织、自调整、自适应的正确位置`。

这些 API 的共同点是：Rust typed plug implementation、graph-local Plug、flow 都是 `Graph` 的能力，并共享同一个 check、commit 和 run 生命周期。public graph API 保持单一入口。

ExecutionPolicy 作为运行配置传入 Graph surface。默认 policy 应足以运行普通 graph；用户只在需要改变失败策略、并发上限或资源级并发限制时显式提供 policy。Picker 选择、seed/manual trigger 和 resume 输入都属于一次 run 的参数。policy、picker 和 seed 变化都不改变 plug、flow 或 GraphStore。

Graph 运行入口保持单一：`run` 是默认可变运行入口，会把运行中产生的 GraphMutationRequest 持久化回调用者 Graph。默认调用写作 `graph.run(json!({ ... })).await?`；带配置调用写作 `graph.run(Run::new(json!({ ... })).policy(policy).picker(picker).seeds(["adapt"])).await?`。`Run` 是一次运行请求，不是新的执行器。

`Run` 负责收束运行参数：initial `Value`、可选 seed plug 列表、`ExecutionPolicy`、公开 `PickerStrategy`。`Run::resume(&previous)` 从上一次 GraphResult 的 outputs 生成本次 initial input；再通过 `.seeds([...])` 指定继续执行的 plug。manual run 不需要独立入口，等价于 `Run::new(initial).seeds(plugs)`。

这些参数不引入新的 kernel 概念，最终都汇入同一 checked Graph + InputBind + ExecutionPolicy 执行模型。

#### 2.2 模块分层标准

##### 2.2.1 目录形态

模块结构按深模块组织，围绕 Graph 聚合调度、plug、flow、check、value、output 和 error。结构如下：

```text
src/
  lib.rs        // crate entrypoint and module exports
  graph.rs      // public Graph API
  plug.rs       // Rust serde plug registration and execution
  check.rs      // Graph declaration checks and derived indexes
  kernel.rs     // scheduler, dependency counters, JoinSet job lifecycle
  value.rs      // JSON value helpers
  flow.rs       // Flow, SourceSelector, InputBind, PlugInput building
  output.rs     // GraphOutput view over GraphResult outputs
  error.rs      // flow-specific errors mapped to CoreError or new CoreError variants
```

Graph 应作为深模块独立成形。kernel 聚焦调度、并发、flow 传播和结果收束。Graph 是主入口，值得拥有高 locality。

##### 2.2.2 错误模型

graph/flow 错误使用结构化错误。错误类别覆盖未知 plug、重复 plug、无效 flow path、required input 缺失、alias 歧义和 Rust plug panic 映射。

##### 2.2.3 GraphResult 与可观测性

最小 kernel 也需要可观察，因为并发 workflow 的错误经常来自输入满足状态、错误传播、tick 顺序和 flow 冲突。Graph 应该产出 `GraphResult`，并在其中附带结构化 events：graph started、tick queued、plug input built、job started、job done、job failed、flow propagated、pending approval、graph idle、duration、tick wait time。`pending approval` 只是普通 plug 输出被记录成可观察事实，不是 kernel 调度停止信号。`tracing` 可以通过 feature-gated Rust integration 接入。

##### 2.2.4 Graph 与 kernel 的关系

`Graph` 是用户面对的 workflow surface。公共入门叙事采用 Graph-first 顺序；扩展能力留在实现层。

##### 2.2.5 并发 Graph kernel 标准

public `Graph` 是能“充分利用多线程和并发”的执行面。调度 kernel 和复杂 flow 推断都汇入同一最终结构。

目标执行模型：`Graph::run` 应由 Runner 驱动。运行状态读取 `Flow` 和节点级反向依赖缓存，并维护 `input_snapshots`、`tick_queue`、`jobs`、`results` 和 `versions`。plug 被 tick 触发后可以并发运行。每个 spawn 的 plug 必须返回 plug name、tick id 和 result。为了通过 `JoinSet`，内部 plug future 输出类型需要同构，例如 `Result<(PlugName, TickId, Value), CoreError>`。如果 plug panic，映射为结构化错误。

##### 2.2.6 graph 深模块标准

graph/plug/flow 能力位于独立 graph 深模块。public API 保持集中；外层只 re-export 必要类型。

模块组织顺序：先定义 graph；再把 `PlugEntry`、`TypedPlug` 放到 plug 模块；把 `GraphOutput` 放到 output 模块；把 graph check 放到 graph 模块；把 InputBind 和 PlugInput 构建放到 flow 模块。每一步都应有行为测试，确保模块边界保持语义稳定。

graph 深模块提供调度、flow planning 之间的 seam，并保持模块 locality。

##### 2.2.7 Plug 与 flow 标准

Graph 存储包含 plug kind 到 plug name 的映射，以及 flow。typed Rust plug 通过 `plugup` 在当前进程中注册 Plug 声明；`plugin` 只把已注册 kind 实例化为 graph-local plug name。flow planning 由 Plug 声明和 flow 规则驱动。

##### 2.2.8 字段 flow 与运行时 PlugInput 构建标准

字段 flow 使用 declarative JSON flow。`flowin` 保存依赖关系和可选字段来源声明；`check` 验证 plug 名和字段路径一致性；`run` 在 plug 收到 tick 时按声明绑定输入。

字段 flow 的 public 写法使用 `flowin(json!(...))`；PlugInput 构建只读取路径、写入路径并报告结构化错误。

##### 2.2.9 自适应 graph 修改标准

自适应 graph 修改由普通 Rust plug 产出 graph 修改请求或 next Graph。宿主通过普通输入提供当前 GraphStore、上一次 GraphResult、资源状态和用户目标等事实；coreflow 只要求这些事实可序列化。默认 `run` 入口应用修改后执行 check 并写入新的 GraphCommit。

### 3. 执行语义与工程边界层

本层把 kernel 运行语义、工程边界和实现代价写清楚。实现者在这里看到状态机、并发细节、field path 语义和性能边界。

#### 3.1 收束

本节只补充实现边界。对象职责归属见 `2. 领域语言`。

`Graph` 的 Runner + Picker 必须具备真正并发能力，并把反馈 flow 的 Idle 行为用测试锁住。因为这直接对应系统核心目的。Graph 存储和 Plug 声明按运行时 Plug registry 设计，把 Rust 编译期能力定位为 Rust 端便利能力。

#### 3.2 Kernel 的形式化语义

为了让实现保持 kernel 形状，kernel 语义写成最小状态机。一个 graph run 的输入是当前 checked Graph snapshot 和 initial value。

Graph snapshot 包含有限 plug 集合 `N`、有向 flow 边集合 `E`、每个 plug 的 executor、每个 plug 的 InputBind 和入口触发事实。

运行状态包含 `tick_queue`、`jobs`、`input_snapshots`、`versions`、`done_ticks` 和 `failed`。初始时，initial value 或 seed 生成 tick。

当 `tick_queue` 非空且 `jobs` 未达到并发上限时，Picker 从队列中取 tick，Runner 启动对应 plug。

plug 成功完成后，结果写入 result table，版本号递增，flow 把变化写入下游 input snapshot；下游 snapshot 如果发生有效变化，就生成新的 tick。

当队列为空且 jobs 为空时，run 进入 Idle。失败策略选择 fail-fast。

这个状态机有几个不变量。第一，同一个 plug 在单次 run 中可以执行多次，但每次执行必须有 tick id 和 input version。

第二，plug 只有在入口触发事实存在且 required input snapshot 已满足时才能开始。第三，每个 done tick 必须在 result table 中产生一个版本化输出，failed tick 必须在 error table 中有错误。

第四，job 必须由 Runner 持有 handle。第五，run 结束时，成功 run 达到 Idle；失败 run 为未完成 tick 记录可解释状态，例如 blocked_by_failure 或 superseded。

第六，自适应 graph 修改生成新 run 使用的新 graph。

把语义写成集合和状态机的好处是测试容易写。测试可以只断言事件序列满足不变量。例如并发测试可以记录 `plug_started` 和 `plug_completed` 时间，证明两个 initial ticks 在任意一个完成前都进入 jobs。反馈测试可以让 A -> B -> A 在第三次输出不变时进入 Idle。失败测试可以证明依赖失败结果的 tick 停留在 blocked_by_failure。

#### 3.3 Check 环节与运行环节必须分离

`Graph::run` 使用 check 和 run 两个环节。check 负责声明一致性和缓存刷新；run 负责 tick、job、flow propagation 和 GraphResult。

#### 3.4 并发执行的实现细节

并发实现最大的 Rust 细节是 ownership。当 plug 存在 `BTreeMap<String, PlugEntry>` 中，并且 plug 是 Rust serde closure 时，多个 ticked plug spawn 到 `JoinSet` 需要可拥有的 plug handle、内部锁或可 clone executor。反馈 flow 还意味着同一个 plug 可能在同一次 run 中收到多次 tick，因此实现必须决定同一 plug 的重复 tick 是串行、可并发，还是由 plug 实现自己的契约决定。

第一种是让 plug entry 存为 `Arc<Mutex<PlugEntry>>`。spawn 时 clone Arc，future 内 lock plug 并调用。优点是实现简单，可支持 stateful `FnMut`，并自然串行化同一 plug 的重复 tick。缺点是每个 plug 调用有锁开销；如果某个 plug 需要同一 plug 多 tick 并发，这个方案会过于保守。

第二种是在 check 后把 plug 索引固定，每次 tick 临时取出 plug ownership，spawn 后结果带回 plug 再放回。优点是少锁，缺点是实现复杂，panic 或 join error 时要保证 plug 归还，确保 graph 可复用；同一 plug 的重复 tick 也需要显式排队。

第三种是要求 `F: Fn(I) -> Fut + Clone + Send + Sync`，每次 tick clone plug。优点是并发自然，适合 stateless plug；缺点是放弃 `FnMut` 能力，也限制 stateful plug。

本标准采用 `Arc<Mutex<_>>` 或 `Arc<tokio::sync::Mutex<_>>` 作为最小可行方案。默认语义是同一 plug 的多个 tick 串行，不同 plug 之间并发。cloneable stateless plug 优化服务于同一 plug 的多个 tick 并发复用。

#### 3.5 并发结果

并发结果必须可解释。`GraphOutput` 按 plug name 读取最新稳定输出，所以最终 snapshot 的定义独立于 completion order。

GraphResult event 字段清单以 `实现 / 2.2.3 GraphResult 与可观测性` 为准。

#### 3.6 Timeout 与 cancel 的位置

Timeout 与 cancel 用普通 plug 建模：`clock` plug 输出当前时间或 deadline 状态，`cancel_source` plug 输出取消请求，业务 plug 读取这些 Value 后输出业务决策。Kernel 把 `expired`、`cancelled`、`route` 和 `status` 当作普通字段名传播。

Fail-fast 只处理 job 失败后的运行收束。Kernel 停止调度依赖失败结果的新 tick，并把已完成结果、失败 tick 和未启动 tick 的状态写入运行结果。用户级取消和 timeout 由 plug 产出的 Value 表达。

远程请求中断、HTTP timeout 或运行时取消由 Rust plug 表达。本项目记录 Rust plug 返回的结果和错误。

#### 3.7 资源与背压

AI workflow 常见问题是并发过多：同时发起太多模型调用、工具调用或文件读写。kernel 的最小控制是 `max_concurrency`。它是资源保护机制，应该属于 kernel options。更细粒度的 rate limit 归属 `ExecutionPolicy`。

如果某个 plug 实现选择声明资源标签，例如 `resource: "openai"`、`resource: "filesystem"`、`resource: "gpu"`，scheduler policy 可以通过 `ExecutionPolicy.resource_limits` 限制每类资源的并发数。资源限制只阻塞同资源且达到上限的 tick，不应阻塞无关资源的 ready work。

背压在 batch tick 模型中相对简单，因为每个 tick 只产出一个结果版本。streaming value 会把 graph run 变成 stream graph runtime；本标准采用 batch edge。需要流式输出时，把流式协议封装在一个 plug 内部。

#### 3.8 Plug 粒度

Unix 的“一个程序只做一件事”在 plug 里对应“一个可命名、可测试、可替换的业务行为”。Plug 粒度保持中等。过细会带来调度开销、schema 噪音和 GraphResult event 噪音。过粗则会隐藏依赖和并发机会。判断标准是：这个函数是否有独立输入输出语义，是否值得单独测试，是否可能被替换，是否产生可复用中间结果，是否给并发调度带来机会。

例如“从用户 JSON 中取 email 字段”适合作为 PlugInput 构建或字段 flow；跨多个 graph 复用的规范化步骤适合成为 plug。“调用模型生成计划”值得成为 plug。“调用三个工具并汇总结果”如果工具之间可并发，应该拆成三个工具 plug 和一个汇总 plug。三个步骤必须共享私有状态且需要整体测试时，保留在一个 plug 里可能更好。

本标准包含“plug 粒度指南”。这样用户会把 flow 当成依赖和数据关系，也会把业务逻辑放进可命名、可测试的 plug。

#### 3.9 显式 flow 声明的边界

flow 声明必须显式提供，而不是从宿主类型自动导出。typed plug 可以直接运行；当 graph 需要提前做字段级校验和 flow planning 时，调用方通过 `plugup` 提供实现。

这条边界让核心执行只依赖显式声明数据，而不依赖额外的宿主类型导出工具。

#### 3.10 Field path 语义

Field path 是面向 Value 的声明式路径，用于从 source output 读取字段并写入 target input。它只表达 object field、array index 和缺失路径错误。

#### 3.11 Alias 与 rename

字段 rename 通过显式 flow 声明进入 flow planner。Plug 声明和 flow planner 的分层见 `标准 / 5.6 Plug 声明与 flow 分层`。

#### 3.12 验证矩阵

从零实现时附带验证矩阵。矩阵至少包含这些测试。

graph check：重复 plug、未知 plug、反馈 flow 存在但不报错、孤立 plug 是否允许。孤立 plug 如果有 manual trigger 或 initial input 也应作为入口运行。

flow check：target plug 不存在、source plug 不存在、target field 重复、required input 没来源、alias 自动匹配无歧义、alias 匹配歧义时报错。

run：入口输入反序列化失败、plug 输出序列化失败、flow path 缺失、target 输入反序列化失败、Rust plug panic 映射。

concurrency：两个 initial ticks 同时启动，fan-out 多下游并发，fan-in 等待 required inputs，fast branch 不等 slow branch，反馈 flow 在输出不变时进入 Idle。

concurrency 还需要覆盖 max_concurrency 限制生效、fail-fast 停止调度依赖失败结果的新 tick、completed partial results 记录在 GraphResult。

reuse：同一个 graph 多次 run 结果隔离，第一次失败后的第二次成功保持独立。

这些测试比单纯快照文档更重要，因为它们定义 kernel 行为。

#### 3.13 示例集设计

文档示例服务学习路径。示例包含这些类型。

最小线性 serde graph 展示 `plugup`、`plugin`、`flowin`、`run`、`output.get`。fan-in graph 让两个 initial ticks 并发生成 contact 和 template，下游 compose 等待二者。

字段 flow 展示上游嵌套 profile、下游 recipient/display_name，并用 `flowin(json!(...))` 或 schema alias 精确绑定字段。

feedback graph 展示 `route_review` 作为普通 plug 输出下一步业务状态，`clock` 或 initial input 作为普通 plug 提供 timeout/cancel 事实。

feedback graph 示例只使用普通 plug：

```text
clock  -> route_review
review -> route_review
route_review -> draft
draft  -> review
route_review -> final_receipt
```

`route_review` 读取 `review` 的反馈和 `clock` 的时间事实，输出普通 JSON：

```json
{ "route": "draft", "prompt": "rewrite with shorter sentences" }
```

或：

```json
{ "route": "final_receipt", "final": "..." }
```

Kernel 按 flow 声明传播字段，例如把 `route`、`prompt` 或 `final` 作为普通 Value 字段处理。

如果 `route_review` 的输出让 `draft` 输入发生变化，`draft` 收到 tick；input snapshot 保持稳定时，graph run 进入 Idle。

`final_receipt` 是普通下游 plug，它负责产出最终结果。Timeout 和 cancel 也按同一方式建模：`clock` 或 `cancel_source` 输出事实，业务 plug 读取这些事实并输出业务决策。

示例集主路径引用 `实现 / 2.1 API 标准：极简 serde Graph`；新用户随后看到 fan-in、字段 flow 和 feedback graph 示例。

#### 3.14 公开类型命名

命名需要反映层次。public workflow 类型：`Graph`、`GraphOutput`、`GraphResult`、`GraphStore`、`Plug`、`Flow`、`PlugKind`、`PlugName`、`InputBind`、`PlugInput`。内部可以有 `Kernel`、`GraphIndexes`。

公共图模型命名为 `Graph`；`flow` 表示 plug 之间的依赖、数据流和反馈关系。公共文档采用 Graph-first 叙事。

对于 workflow function，Rust API 用 `plugup` 注册实现、用 `plugin` / `plugout` 加入或删除 graph-local plug，文档统一叫 plug。

#### 3.15 持久化边界

很多 workflow engine 会很快加入数据库、工作状态表、重试队列和 cron。持久化边界见 `标准 / 4.1 Graph 文件存储模型`。

可恢复 workflow 通过新的 run 输入恢复；kernel 只消费 Graph、run 输入和运行内状态。

这同样符合微内核：核心记录机制事件，持久化策略归属 Graph 文件存储边界。

#### 3.16 Pause 与人工审批

AI workflow 很可能需要人工审批。人工审批建模为一个 plug，它返回 `PendingApproval`。GraphResult event 记录等待审批的事实；这个事件不让 kernel 暂停或取消其他独立 work。恢复通过新的 run 输入表达。

Graph 文档中只说人工审批可以作为 Rust plug。typed plug 保持函数式业务模型。

#### 3.17 安全边界

运行时字段 selector 会带来安全问题。字段 path selector 必须是数据，只执行字段选择。schema validator 采用声明式规则。

Plug 要接受运行时校验。一个 plug 可以宣称输出 schema 有字段 `email`，实际返回别的结构。kernel 仍然要在运行时按实际 value 检查 path 和反序列化。schema 用于提前校验和文档，安全边界由运行时检查、capability 和 sandbox 承载。

#### 3.18 性能边界

使用 `serde_json::Value` 会有分配和拷贝成本。对于 AI workflow，这通常可以接受，因为模型调用和 I/O 远大于 PlugInput 构建开销。设计要减少 clone。Fan-out 时同一个上游输出被多个下游读取，可以内部用 `Arc<Value>`，构建 PlugInput 时 clone Arc。需要修改 target input object 时才创建新 object。

对于单依赖透传，如果 target plug 直接消费 source output，PlugInput 构建可以移动 value。多个 downstream 共享输出时，PlugInput 构建使用共享引用或 clone 策略。实现以 benchmark 为依据优化，保持可读性。

并发调度开销也要注意。对于很短的 CPU 小 plug，spawn 开销可能大于 plug 本身。inline fast path 处理这类情况：如果 tick queue 只有一个 tick，或者 policy 设置 `inline_small_plugs`，可以直接 await 而不 spawn。正确并发和优化同属本标准。

### 4. 实现、文档与评审层

本层负责收尾。它给实现者一组落地结构、文档要求、评审清单和最终判断，让标准从设计语言变成实施语言。

#### 4.1 能力结构

标准拆成明确能力层，并以能力边界指导发布节奏。

能力层只用于组织实施顺序；public API 见 `实现 / 2.1 API 标准：极简 serde Graph`，行为验收见 `实现 / 3.12 验证矩阵`。

#### 4.2 设计风险

最大风险是范围膨胀。Plug 声明、GraphStore、GraphResult 和 ExecutionPolicy 必须各自保持清晰边界。GraphStore/GraphResult 的边界见 `标准 / 4.1 Graph 文件存储模型`。

第二个风险是过度 Rust 类型化。纯 Rust 类型级 DAG 很漂亮，但会挤压 GraphStore/Plug 这种可导出的运行时结构。

第三个风险是过度动态化。为了动态执行放弃 Rust 类型会削弱 Rust 用户体验。解决方法是本项目聚焦强类型 Rust API。

第四个风险是 flow DSL 变成编程语言。解决方法是让 selector 只表达字段选择，业务转换一律由 plug 表达。

第五个风险是并发语义不清。解决方法是用状态机和测试固定语义。

#### 4.3 目标 public API 稳定策略

目标 public API 以 `Graph` 作为公共图模型；核心入口见 `实现 / 2.1 API 标准：极简 serde Graph`。

因此，public API 应保持窄接口。`PickerStrategy` 是公开调度选择；具体 `Picker` trait、队列结构和 scheduler 实现保持内部可见；其余实现细节归入对应 seam。

文档中必须明确核心标准和扩展能力的边界。每次 public API 变化都必须对应本标准中的某个能力层和测试。

#### 4.4 Rust API 与 graph 文件协议的双层承诺

Rust API 承诺的是 ergonomics：用户写 typed async function，derive serde，通过 `plugup` 注册到 Graph，再通过 `plugin` 作为 graph-local plug 使用。graph 文件协议承诺的是 protocol：plug kind 到 plug name 的映射、flow 和 GraphCommit。运行时协议承诺的是 Plug、Value、InputBind 和 error。两者保持双层边界。

例如 Rust API 可以允许：

```rust
graph.plugup("send_email", send_email)?;
graph.plugin("send", "send_email")?;
```

graph 文件协议则必须表达为：

```json
{
  "plugs": {
    "send_email": ["send"]
  },
  "flow": {}
}
```

Rust API 可以自动生成第二个结构，但 kernel 运行时看的是第二个结构。这种双层承诺能同时满足 Rust 最简使用和 graph 文件可导出。

#### 4.5 Graph 存储与 Plug 声明叙事标准

Graph 存储叙事围绕单一 `graph.json`、当前完整 Graph 和 GraphCommit 更改记录展开。PlugInput 构建叙事围绕 declarative selector、InputBind 和 serde 反序列化展开。自适应叙事围绕普通 Rust plug 产出的 graph 修改请求展开。

#### 4.6 文档标准

公共文档采用 Graph-first 叙事：注册 plug、声明 flow、运行 graph、读取 GraphOutput。

Key concepts 按 `2. 领域语言` 排序。行为测试名必须表达行为，例如 `graph_runs_ticked_plugs_concurrently`。

#### 4.7 最小可实施结构

最小可实施结构如下。

结构 1：`Graph::run` 采用 Runner + Picker。结构 2：graph 成为独立深模块。结构 3：存储协议接入 GraphStore。结构 4：PlugInput 构建接入 InputBind。结构 5：公共叙事接入 Graph-first 文档。

每个结构都能独立验证，并共同组成最终架构。

#### 4.8 最小不变量清单

实现者检查这些不变量。

一，plug 只引用自己的输入输出。二，flow 关系写在 Graph。三，kernel 传播业务字段但解释权归属 Rust plug 和 PlugInput 构建。

四，schema 生成归属 Rust 端便利层。五，收到 tick 的 plug 默认并发。六，所有 job 都由 Runner 收割。

七，GraphOutput 按 plug name 稳定读取最新输出。八，flow 可以有环，环仍按输入变化传播，并通过 Idle 结束。

九，GraphStore 可导出。十，自适应归属见 `标准 / 5.5 自组织、自调整、自适应的正确位置`。

这些不变量比任何单个 API 形状更重要。API 必须服从不变量。

#### 4.9 一个更完整的 Runner 状态机伪代码

```rust
async fn run_graph(graph: &mut Graph, initial: Value) -> Result<GraphResult, GraphError> {
    let mut state = RunState::new(graph, initial);
    let mut jobs = JoinSet::new();

    while state.is_running() {
        while jobs.len() < graph.policy.max_concurrency && state.has_tick() {
            let tick = graph.policy.pick(&mut state.tick_queue);
            let plug_id = tick.plug_id;
            let input = graph.input_binds[plug_id].build_plug_input(&state.input_snapshots)?;
            let plug = graph.plugs[plug_id].plug.clone_handle();
            state.mark_started(&tick);
            jobs.spawn(async move {
                let value = plug.call(input).await;
                JobOutcome { tick, value }
            });
        }

        if state.tick_queue.is_empty() && jobs.is_empty() {
            return Ok(state.finish_idle());
        }

        let Some(joined) = jobs.join_next().await else {
            return state.finish_if_terminal();
        };

        match joined {
            Ok(JobOutcome { tick, value: Ok(value) }) => {
                let changed = state.done_tick(&tick, value);
                if changed {
                    state.propagate_flow(&tick, graph)?;
                }
            }
            Ok(JobOutcome { tick, value: Err(error) }) => {
                state.fail_tick(&tick, error);
                if graph.policy.failure == FailurePolicy::FailFast {
                    state.stop_scheduling_new_ticks();
                    while let Some(joined) = jobs.join_next().await {
                        state.record_in_flight_job(joined);
                    }
                    return Err(state.finish_failed());
                }
            }
            Err(join_error) => {
                state.record_join_error(join_error);
                jobs.abort_all();
                drain_jobs(&mut jobs, &mut state).await;
                return Err(state.finish_failed());
            }
        }
    }

    state.finish_if_terminal()
}
```

这段伪代码表达的是控制权所有权：Runner 持有 jobs，所有 plug 都通过 Runner 收割。后台工作通过 Runner job 生命周期表达。

#### 4.10 一个更完整的 PlugInput 构建伪代码

```rust
fn build_plug_input(
    input_bind: &[FieldFlow],
    results: &BTreeMap<PlugName, Value>,
) -> Result<Value, GraphError> {
    let mut target = Value::Object(Default::default());

    for flow in input_bind {
        let source_root = results
            .get(&flow.source_plug)
            .ok_or_else(|| GraphError::MissingSourcePlug(flow.source_plug.clone()))?;

        let source_value = read_path(source_root, &flow.source_path)
            .ok_or_else(|| GraphError::FlowPathNotFound(flow.clone()))?;

        write_path(&mut target, &flow.target_field, source_value.clone())?;
    }

    Ok(target)
}
```

这里表达 PlugInput 构建职责。读取路径、写入路径、错误报告，是 PlugInput 构建的全部职责。类型校验发生在 schema preflight 和最终 deserialize。

#### 4.11 最终架构图

```text
Rust typed fn / closure
     │
     ▼
serde decode/encode + Plug
     │
     ▼
Graph API
     │
     ▼
Graph + InputBind + ExecutionPolicy
     │
     ▼
Graph kernel + indexes
     │
     ▼
tick queue + JoinSet jobs + versioned result table + GraphResult
     │
     ▼
GraphOutput
```

这张图的重点是 Rust plug 在 kernel 前收敛到 serde value、Plug 和 InputBind。kernel 之后产出 GraphOutput 与 GraphResult。

#### 4.12 一句话标准

`coreflow` 的一句话标准应当是：向 Graph 注册单一职责 plug，用 `flowin`/`flowout` 声明可形成反馈环的 flow；flow 同时表达 plug 依赖关系和可选字段 selector，kernel 在输入变化产生 tick 时并发执行 plug，并通过 serde-compatible value protocol 在 plug 之间传递结构化数据，直到 Idle 或 failure。

这句话同时固定了范围：Graph、plug、flow、kernel、并发、value protocol、GraphStore 和 Plug。这就是最小 AI workflow 程序应该守住的范围。

#### 4.13 设计评审清单

任何实现都可以用 `实现 / 4.8 最小不变量清单` 评审。新增能力必须先回答归属问题：它是否必须进入 kernel 才能保证依赖和调度正确性。

这份清单的作用是让系统在多人或多 agent 实现时不漂移。每个人都可能觉得“只加一个小功能”没有伤害，但 workflow 框架就是这样膨胀的。只要每次都问“这个功能是否属于 kernel”，系统就能保持微内核风格。

#### 4.14 为什么标准优先于代码

标准优先于代码的详细理由见 `实现 / 4.12 一句话标准` 和 `实现 / 4.8 最小不变量清单`。

#### 4.15 给实现者的最后提醒

实现时最容易犯的错误，是把“方便”误认为“核心”。真正的核心判断见 `实现 / 4.12 一句话标准`；Rust 用户体验与 graph 文件协议的边界见 `实现 / 4.4 Rust API 与 graph 文件协议的双层承诺`。

## 其他

### 1. 边界与对比层

本层回答两个问题：系统边界放在哪里，它和同类 Rust DAG 库有什么差别。你读完这一层，应该能评审设计取舍。

#### 1.1 边界选择

Plug 保持函数模型。Actor 模型适合持续存在、持有状态、处理消息的实体；这里的 plug 更像函数：接收输入、返回输出或错误。保持函数模型可以减少 mailbox、lifecycle tree、消息协议和内部路由带来的额外概念。

字段 flow 保持声明式数据选择。selector 只描述从哪里取值、写到哪里；运行时字段 selector 的安全边界见 `实现 / 3.17 安全边界`。

schema 采用小子集。完整 JSON Schema 包含 `$ref`、composition、conditional、format、自定义 keyword 等复杂能力；基础标准表达对象字段、required 和基本类型，足够支撑 Plug、preflight validation 和 flow planning。

Rust 类型体验是本项目的主要 ergonomics。完全类型级 DAG 可以很漂亮，例如 `dagx` 的 typed handle 和 compile-time cycle prevention；`coreflow` 的核心协议仍以 Rust serde plug、Plug 和 Value 为边界。

应用层 workflow 以 `Graph` 为公共模型。Rust typed plug、schema 和 flow 能力都汇入 `Graph`。

#### 1.2 与 Rust DAG 库的对比

`dagx` 代表强 Rust 类型系统路线。它使用 typed handle、宏和 type-state，强调 compile-time validation、runtime-agnostic 和 optimal parallel execution。这对纯 Rust 库非常有吸引力。`coreflow` 采用它的并发执行、inline fast path、typed handle 思想，同时把核心校验建立在 Plug、Value 和 InputBind 上。

`taski` 代表 async work DAG 路线。它描述每个异步工作单元、typed dependency 和 executor 并发运行 ready jobs 的模型。这与 `coreflow` 的 kernel 非常接近。可学习点是 policy seam、max concurrency、trace。不同点是 `coreflow` 的 public language 叫 Graph/plug/flow，并且需要 GraphStore 和 Rust serde value boundary。

Tokio `JoinSet` 是 Runner 持有 jobs 的合适构件。它保证 job 由 Runner 持有，按完成顺序 join，drop 会 abort，`abort_all` 后需要继续 drain。`coreflow` 的并发 kernel 借鉴这个生命周期模型，所有 job 都由 Runner 收割。

### 2. 示例与附录层

本层放示例、附录和背景压缩材料。前面几层先定边界和结构；这一层负责把抽象判断落到例子和补充说明上。

#### 2.1 附录：一条端到端示例

端到端场景直接复用 `实现 / 2.1 API 标准：极简 serde Graph` 和 `实现 / 3.13 示例集设计`，附录只保留引用入口。

#### 2.2 附录：参考材料摘要

参考材料用于校准概念来源：Bell Labs Unix history 校准组合原则；L4 与 QNX 校准微内核边界；Tokio `JoinSet` 校准 job lifecycle；Serde data model 校准 Rust value boundary；JSON Schema 校准 Plug schema 子集；Rust DAG 库校准 ready-job 并发和 policy seam。
