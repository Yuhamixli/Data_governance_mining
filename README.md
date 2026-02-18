# Data Governance for AI Agents

面向 AI Agent 的数据治理框架，为 [xnobot](https://github.com/user/xnobot) 等 AI 助手提供知识库数据质量管理能力。

## 为什么需要数据治理？

AI 助手的知识库会随时间不断累积数据（文档、网页缓存、聊天记录、记忆文件），如果缺乏治理机制，将面临：

| 风险 | 影响 |
|------|------|
| **重复数据** | 搜索结果冗余，检索质量下降 |
| **过期数据** | 助手给出过时甚至错误的回答 |
| **损坏数据** | 编码错误、空内容导致搜索噪音 |
| **缺失血缘** | 无法追踪问题数据的来源 |
| **无质量度量** | 数据质量持续恶化却无法察觉 |

本框架提供**自动化、可声明、Agent 可调用**的数据治理能力，同时保留完整的人工审查与干预通道。

## 设计哲学

> 数据是 AI 进化的燃料。治理好数据，就是为 AI 铺平进化之路。

本项目采用 **AI-first, Human-friendly** 的设计原则：

- **AI 自主运行**: GovernanceAgent 能自主感知、推理、决策、行动，不依赖人类触发
- **人类随时介入**: 所有过程对人类透明可读，CLI / 报告 / 审计日志一应俱全
- **质量即数据**: 质量分数不只是报告，而是嵌入数据本身的元数据，Agent 检索时自动感知
- **渐进式采用**: 可以只用去重，也可以开启全套自主治理，由你决定自动化程度

```
┌─────────────────────────────────────────────────┐
│          Human Interface (人类介入层)              │
│  CLI 命令行 │ Markdown 报告 │ 审计日志 │ 告警     │
├─────────────────────────────────────────────────┤
│          AI Agent Interface (Agent 接口层)        │
│  GovernanceToolkit │ 结构化 JSON │ 事件钩子       │
├─────────────────────────────────────────────────┤
│          Autonomous Layer (自主治理层)             │
│  GovernanceAgent │ GovernanceDaemon │ DataPassport│
├─────────────────────────────────────────────────┤
│          Governance Engine (治理引擎层)            │
│  Profiler │ Dedup │ Validation │ Freshness        │
│  Lineage  │ Reporter │ Alerts                     │
├─────────────────────────────────────────────────┤
│          Data Sources (数据源层)                   │
│  ChromaDB │ Knowledge Files │ Chat History │ Memory│
└─────────────────────────────────────────────────┘
```

## 核心能力

### 1. 数据质量画像 (Profiler)

对知识库进行多维度质量评估，输出人可读的报告 + 机器可解析的结构化数据。

- **文档级**: 完整性、有效性、新鲜度评分
- **Chunk 级**: 空内容、过短/过长、乱码、重复检测
- **集合级**: 跨文档/向量库的一致性评估，文件与向量的匹配度

```python
from data_governance.api import GovernanceFacade

gov = GovernanceFacade(
    workspace_path="~/.xnobot/workspace",
    chromadb_path="~/.xnobot/workspace/knowledge_db",
)

# 整体画像 — 返回 QualityReport，支持 .to_summary() 人读 / .model_dump() 机读
report = gov.profile_knowledge_base()
print(report.to_summary())  # 人类可读的摘要
```

### 2. 去重引擎 (Dedup)

两阶段去重，安全高效：

- **精确去重**: xxHash 内容哈希，O(n) 复杂度，发现完全相同的内容
- **语义去重**: Embedding 余弦相似度，发现语义等价的近似重复
- **智能保留**: 保留元数据最丰富的副本，删除冗余

```python
# 先查看（dry run），再决定是否删除
dedup_report = gov.find_duplicates(include_semantic=False)
print(dedup_report.summary())  # 人类审查

# 确认后执行删除
gov.remove_duplicates()
```

### 3. 校验框架 (Validation)

声明式规则引擎，内置 4 套规则集，支持自定义扩展：

| 规则集 | 校验内容 |
|--------|---------|
| `knowledge_chunks` | 空内容、过短/过长、编码损坏、缺失元数据 |
| `document_files` | 文件存在性、格式支持、UTF-8 编码、大小限制 |
| `chat_history` | JSONL 语法、消息结构、角色字段 |
| `memory` | 内容非空、Markdown 结构 |

```python
# 校验全部数据资产
results = gov.validate_knowledge_base()
print(gov.validation_engine.summary(results))  # 人类可读摘要

# 自定义规则
from data_governance.validation import ValidationRule

my_rule = ValidationRule(
    name="my_check", description="Custom check"
).with_check(
    check_fn=lambda content="", **kw: "敏感词" not in content,
    message_fn=lambda **kw: "包含敏感内容",
)
```

### 4. 新鲜度追踪 (Freshness)

按数据类别执行不同的新鲜度策略：

| 数据类别 | 陈旧阈值 | 过期后操作 |
|---------|---------|----------|
| 长期知识 | 90 天 | 提醒审查 |
| 短期知识 | 15 天 | 归档 |
| Web 缓存 | 3 天 | 删除 |
| 聊天记录 | 90 天 | 归档 |
| 每日记忆 | 180 天 | 提醒审查 |

```python
freshness = gov.check_freshness()
print(freshness.summary())

stale = gov.get_stale_assets()    # 陈旧但未过期
expired = gov.get_expired_assets() # 已过期，含推荐操作
```

### 5. 数据血缘 (Lineage)

追踪数据从源文件到向量库的完整链路，支持影响分析：

```python
# 追踪某个 chunk 的来源
lineage = gov.get_lineage("chunk:abc123", direction="upstream")

# 导出 Mermaid 图表（人类可视化）
diagram = gov.get_lineage_diagram()
```

### 6. 健康报告与告警 (Reporter)

综合所有维度生成加权健康评分，并触发告警：

```python
health = gov.health_check()

# 人类可读：Markdown 报告
print(health.to_markdown())

# 机器可读：结构化字典
data = health.to_dict()
# {"overall": 0.72, "status": "WARNING", "components": [...]}

# 保存历史（支持趋势分析）
# 自动保存到 .governance/health_history.jsonl
```

## AI 自主能力 (Agent-Native)

以上 6 大治理引擎是基础。在此之上，本项目提供 3 个面向 AI 的高阶能力，让治理可以自主运行：

### GovernanceAgent — 自主治理大脑

一个有**感知-推理-决策-行动-记忆**循环的治理 Agent：

```python
from data_governance.agent import GovernanceAgent

agent = GovernanceAgent(
    workspace_path="~/.xnobot/workspace",
    chromadb_path="~/.xnobot/workspace/knowledge_db",
)

# 完整自主循环
plan = agent.perceive_and_decide()

# plan.decisions 示例:
# - DELETE_DUPLICATES (confidence: 99%, severity: HIGH)
# - DELETE_EMPTY_CHUNKS (confidence: 95%, severity: HIGH)
# - SCHEDULE_REVIEW for stale assets (confidence: 80%)

# 人类审查决策计划
print(plan.to_structured_output())           # 结构化查看
for d in plan.needs_approval_decisions():    # 查看需要审批的决策
    print(d.to_agent_message())

# 只自动执行安全操作（去重、删空），危险操作等人确认
results = agent.execute_plan(plan, auto_only=True)
```

**人类介入点**: `plan.needs_approval_decisions()` 返回需要人工审批的危险操作，`auto_only=True` 确保只自动执行安全操作。

### GovernanceDaemon — 持续守护进程

可嵌入 xnobot heartbeat 的后台守护者，支持事件驱动：

```python
from data_governance.daemon import GovernanceDaemon

daemon = GovernanceDaemon(
    workspace_path="~/.xnobot/workspace",
    chromadb_path="~/.xnobot/workspace/knowledge_db",
    check_interval_seconds=3600,          # 每小时检查一次
    on_alert=lambda alert: notify(alert), # 告警回调（可通知人类）
)

# 嵌入 xnobot heartbeat
result = daemon.tick()

# 事件钩子：文档摄入后自动质检 + 去重 + 血缘记录
result = daemon.on_ingest(file_path="doc.md", chunk_ids=["c1", "c2"])

# 事件钩子：搜索前质量门禁
filtered = daemon.on_search(query="...", results=raw_results, min_quality=0.3)
```

**人类介入点**: `on_alert` 回调可以推送通知到管理员；`tick()` 返回结构化结果可以写入日志供人查阅。

### DataPassport & QualityEmbedder — 数据质量协议

让质量信息**成为数据的一部分**，而不是独立的报告：

```python
from data_governance.protocol import QualityEmbedder, DataPassport, PassportRegistry

# 为所有 chunk 嵌入质量分数到 ChromaDB metadata
embedder = QualityEmbedder()
embedder.embed_quality_scores(chromadb_path, "xnobot_kb")
# 之后每个 chunk 的 metadata 包含:
# quality_score, freshness_score, content_hash, is_quarantined, governance_ts

# Agent 检索时自动过滤低质量数据
filtered = QualityEmbedder.quality_aware_filter(
    raw_results, min_quality=0.3, exclude_quarantined=True
)

# 数据护照：每条数据的质量身份证
registry = PassportRegistry(persist_path=".governance/passports.json")
passport = registry.create_passport(
    asset_id="chunk:abc123", content="...", source_type="file"
)
passport.assess(quality_score=0.85, freshness_score=0.9)
# trust_level 自动计算: VERIFIED / TRUSTED / SUSPECT / QUARANTINED
```

**人类介入点**: `QualityEmbedder.get_quality_summary()` 提供聚合统计供人审查；`PassportRegistry.stats()` 展示全局信任度分布。

## 使用方式

本项目提供 3 种使用方式，适配不同场景和角色：

### Python API（开发者 / 集成）

```python
from data_governance.api import GovernanceFacade

gov = GovernanceFacade(
    workspace_path="~/.xnobot/workspace",
    chromadb_path="~/.xnobot/workspace/knowledge_db",
    collection_name="xnobot_kb",
)

# 综合健康检查
health = gov.health_check()
print(health.to_markdown())

# 查找 + 清理重复
gov.find_duplicates()
gov.remove_duplicates()

# 校验 + 新鲜度 + 血缘
gov.validate_knowledge_base()
gov.check_freshness()
gov.get_lineage("file:doc.md")
```

### CLI 命令行（运维 / 人工巡检）

```bash
# 综合健康检查（输出 Markdown 报告）
dg health -w ~/.xnobot/workspace -c ~/.xnobot/workspace/knowledge_db

# 查找重复（默认 dry run，安全查看）
dg dedup -c ~/.xnobot/workspace/knowledge_db

# 确认后执行去重
dg dedup -c ~/.xnobot/workspace/knowledge_db --execute

# 校验全部数据资产
dg validate -w ~/.xnobot/workspace -c ~/.xnobot/workspace/knowledge_db -t all

# 新鲜度检查
dg freshness -w ~/.xnobot/workspace

# 查看活跃告警
dg alerts -w ~/.xnobot/workspace

# 单文档质量评估
dg profile-doc path/to/document.md
```

### Agent 工具（xnobot 注册）

11 个工具可直接注册到 xnobot 的 tool registry，Agent 自主调用：

```python
from data_governance.api.tools import GovernanceToolkit

toolkit = GovernanceToolkit(
    workspace_path=workspace_path,
    chromadb_path=chromadb_path,
)

# 注册工具
for tool_def in toolkit.get_tool_definitions():
    register_tool(tool_def)

# Agent 调用 — 返回结构化 JSON
result = toolkit.execute("governance_agent_cycle", {"auto_execute": True})
result = toolkit.execute("governance_state", {})
result = toolkit.execute("governance_validate", {"target": "all"})
```

| 工具名称 | 描述 | 适用场景 |
|---------|------|---------|
| `governance_agent_cycle` | 完整自主治理循环 | 定期巡检 / 自动维护 |
| `governance_state` | 获取治理状态快照 | Agent 决策前了解数据健康度 |
| `governance_health_check` | 综合健康检查 | 用户问"知识库怎么样" |
| `governance_embed_quality` | 嵌入质量到数据 | 摄入后 / 定期刷新 |
| `governance_find_duplicates` | 查找重复 | 发现检索结果冗余时 |
| `governance_remove_duplicates` | 删除重复 | 确认后清理 |
| `governance_validate` | 校验数据 | 摄入前质量门禁 |
| `governance_check_freshness` | 新鲜度检查 | 定期 / 回答不准时排查 |
| `governance_profile_document` | 文档质量门禁 | 摄入前评估 |
| `governance_get_alerts` | 获取告警 | 管理员巡检 |
| `governance_get_lineage` | 数据血缘查询 | 追踪数据来源 |

## xnobot 集成指南

### 方案 A：Heartbeat 集成（推荐）

在 xnobot 的 heartbeat 中嵌入持续治理：

```python
from data_governance.daemon import GovernanceDaemon

daemon = GovernanceDaemon(
    workspace_path=workspace_path,
    chromadb_path=chromadb_path,
)

async def on_heartbeat_tick():
    result = daemon.tick()
    if result["alerts"]:
        logger.warning(f"Governance: {result['summary']}")
```

### 方案 B：检索前质量门禁

在知识库搜索结果返回前过滤低质量数据：

```python
from data_governance.protocol.quality_embed import QualityEmbedder

def search(query: str, top_k: int = 5):
    raw_results = collection.query(query_texts=[query], n_results=top_k * 2)
    filtered = QualityEmbedder.quality_aware_filter(
        raw_results, min_quality=0.3, exclude_quarantined=True
    )
    return filtered
```

### 方案 C：摄入时自动治理

文档摄入后自动执行质量检查、去重和血缘记录：

```python
def knowledge_ingest(file_path: str):
    chunks, ids = ingest_document(file_path)
    daemon.on_ingest(file_path=file_path, chunk_ids=ids)
```

## 安装

```bash
# 从源码安装
pip install -e .

# 安装开发依赖
pip install -e ".[dev]"

# 安装全部依赖（含语义去重）
pip install -e ".[all]"
```

## 项目结构

```
src/data_governance/
├── core/               # 核心模型与配置
│   ├── config.py       # GovernanceConfig（Pydantic 配置）
│   └── models.py       # DataAsset, QualityReport, LineageGraph 等
├── profiler/           # 数据质量画像
│   ├── metrics.py      # 质量度量（完整性、有效性、熵、乱码检测等）
│   ├── document.py     # 文档级画像
│   ├── chunk.py        # Chunk 级画像
│   └── collection.py   # 集合级画像（文档 + 向量库联合评估）
├── dedup/              # 去重引擎
│   ├── hash_dedup.py   # 精确哈希去重（xxHash）
│   ├── semantic_dedup.py # 语义相似度去重（Embedding 余弦）
│   └── engine.py       # 统一去重流水线
├── validation/         # 校验框架
│   ├── rules.py        # 声明式规则定义（ValidationRule, RuleSet）
│   ├── builtin.py      # 内置规则集（4 套）
│   └── engine.py       # 校验执行引擎
├── freshness/          # 新鲜度管理
│   ├── tracker.py      # 新鲜度扫描与追踪
│   └── policies.py     # 过期策略定义
├── lineage/            # 数据血缘
│   └── tracker.py      # 血缘记录、查询、Mermaid 导出
├── reporter/           # 健康报告
│   ├── health.py       # 健康评分聚合 + Markdown 报告
│   └── alerts.py       # 告警管理（生成、确认、解决）
├── agent/              # AI 自主治理层
│   ├── governance_agent.py  # GovernanceAgent（感知-推理-决策-行动-记忆）
│   └── decisions.py         # Decision, ActionPlan 决策模型
├── daemon/             # 持续监控守护进程
│   └── monitor.py      # GovernanceDaemon（后台守护 + 事件钩子）
├── protocol/           # 数据质量通信协议
│   ├── quality_embed.py    # QualityEmbedder（质量分数嵌入 ChromaDB）
│   └── data_passport.py    # DataPassport（数据可信度护照）
└── api/                # 对外接口
    ├── facade.py       # GovernanceFacade（统一 Python API）
    ├── tools.py        # GovernanceToolkit（Agent 工具定义）
    └── cli.py          # CLI 命令行
```

## 设计原则

1. **轻量级**: 核心依赖极少（pydantic, xxhash, numpy, chromadb），无需部署重量级平台
2. **AI 自主 + 人类可控**: Agent 能自动运行，但人类可以随时审查、干预、否决
3. **声明式**: 校验规则、新鲜度策略均为声明式定义，易于理解和扩展
4. **渐进式**: 可单独使用任一模块，也可通过 GovernanceAgent 全量自动运行
5. **xnobot 适配**: 直接支持 ChromaDB、JSONL 聊天记录、Markdown 记忆文件等 xnobot 数据格式
6. **双通道输出**: 每个操作同时产出人可读（Markdown/Summary）和机器可读（JSON/Dict）两种格式

## 与企业级方案的对比

| 特性 | 本框架 | OpenMetadata | Great Expectations |
|------|--------|-------------|-------------------|
| 部署复杂度 | `pip install` | Docker + MySQL + ES | `pip install` |
| 适用场景 | AI 助手知识库 | 企业数据平台 | 数据管道 |
| Agent 自主治理 | 原生支持 | 不支持 | 不支持 |
| ChromaDB 支持 | 原生支持 | 不支持 | 不支持 |
| 人类可读报告 | Markdown + CLI | Web UI | Data Docs |
| 中文支持 | 原生 | 有限 | 无 |
| 资源占用 | < 50MB | > 2GB | < 200MB |

## 参考

- [OpenMetadata](https://github.com/open-metadata/OpenMetadata) — 开源统一元数据平台
- [Great Expectations](https://github.com/great-expectations/great_expectations) — 声明式数据校验
- [OpenLineage](https://github.com/OpenLineage/OpenLineage) — 数据血缘标准协议

## License

MIT
