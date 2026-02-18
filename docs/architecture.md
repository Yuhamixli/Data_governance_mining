# Architecture: AI-First, Human-Friendly Data Governance

## 设计背景

### nanobot 数据流分析

```mermaid
graph LR
    subgraph Sources["数据源"]
        DOC["文档文件<br/>(.txt .md .pdf .docx .xlsx)"]
        WEB["Web 搜索结果"]
        CHAT["聊天记录<br/>(.jsonl)"]
        MEM["Agent 记忆<br/>(.md)"]
    end

    subgraph Processing["处理层"]
        CHUNK["文本分块<br/>(512 tokens, 200 overlap)"]
        EMBED["向量嵌入<br/>(BGE-small-zh-v1.5)"]
        CACHE["Web 缓存"]
    end

    subgraph Storage["存储层"]
        KB["ChromaDB<br/>nanobot_kb"]
        WKB["ChromaDB<br/>nanobot_kb_web_cache"]
        FS["文件系统<br/>knowledge/"]
        CHATFS["JSONL 文件<br/>chat_history/"]
        MEMFS["Markdown 文件<br/>memory/"]
    end

    subgraph Consumption["消费层"]
        SEARCH["语义搜索"]
        CONTEXT["上下文构建"]
        LLM["LLM 生成回答"]
    end

    DOC --> CHUNK --> EMBED --> KB
    WEB --> CACHE --> EMBED --> WKB
    DOC --> FS
    CHAT --> CHATFS
    MEM --> MEMFS
    KB --> SEARCH --> CONTEXT --> LLM
    WKB --> SEARCH
    MEMFS --> CONTEXT
```

### 识别的数据质量风险

| # | 风险 | 数据层 | 影响等级 | 治理措施 |
|---|------|--------|---------|---------|
| 1 | 重复摄入 | 向量库 | 高 | Hash/语义去重 |
| 2 | 空/过短 Chunk | 向量库 | 高 | 校验规则 + 过滤 |
| 3 | 编码损坏 | 文档/向量 | 中 | 编码检测 + 告警 |
| 4 | 路径不一致 | 元数据 | 中 | 路径标准化校验 |
| 5 | Web 缓存过期 | 向量库 | 中 | TTL 策略 + 自动清理 |
| 6 | 长期知识陈旧 | 文档 | 中 | 定期审查提醒 |
| 7 | 聊天记录损坏 | JSONL | 低 | JSON 校验 + 修复 |
| 8 | 记忆膨胀 | 文件系统 | 低 | 新鲜度追踪 + 归档 |
| 9 | 无数据血缘 | 全局 | 中 | 血缘追踪 |

## 治理框架设计

### 分层架构

```
┌──────────────────────────────────────────────┐
│        Human Interface (人类介入层)            │
│  CLI 命令行 │ Markdown 报告 │ 审计日志 │ 告警 │
├──────────────────────────────────────────────┤
│        Agent API Layer (Agent 接口层)         │
│  GovernanceToolkit │ GovernanceFacade │ JSON  │
├──────────────────────────────────────────────┤
│        Autonomous Layer (自主治理层)           │
│  GovernanceAgent │ GovernanceDaemon           │
│  DataPassport    │ QualityEmbedder            │
├──────────────────────────────────────────────┤
│        Governance Engine (治理引擎层)          │
│  Profiler │ Dedup │ Validation │ Freshness    │
│  Lineage  │ Reporter + Alerts                │
├──────────────────────────────────────────────┤
│        Core Models (核心模型层)                │
│  DataAsset │ QualityReport │ ValidationResult │
│  LineageGraph │ GovernanceConfig │ Decision   │
├──────────────────────────────────────────────┤
│        Data Sources (数据源层)                 │
│  ChromaDB  │  Files  │  JSONL  │  Markdown   │
└──────────────────────────────────────────────┘
```

关键设计：人类介入层在最顶层，确保所有自主操作对人类透明可审查。
自主治理层在中间，可以独立运行，也可以在人类监督下运行。

### 治理流水线

日常运行的推荐流水线:

```
1. health_check()          # 综合评估
   ├── profile_knowledge_base()  # 质量画像
   ├── validate_chunks()         # 规则校验
   ├── find_duplicates()         # 去重扫描
   └── check_freshness()         # 新鲜度扫描
       └── compute_health()      # 聚合评分
           └── check_alerts()    # 触发告警

2. (可选) remove_duplicates()    # 清理重复
3. (可选) archive/delete expired # 清理过期

结果:
├── .governance/health_history.jsonl  # 健康趋势
├── .governance/lineage.json          # 数据血缘
└── .governance/alerts.json           # 告警记录
```

### nanobot 集成方式

推荐三种集成模式，可组合使用:

#### 模式 A: Agent 工具集成（Agent 自主调用）
将治理工具注册为 nanobot 的工具，让 Agent 自主调用:
- Agent 可以在摄入文档前先 `governance_profile_document` 评估质量
- 定期通过 heartbeat 自动执行 `governance_agent_cycle`
- 用户可以直接问 "检查知识库健康度" 来触发

#### 模式 B: Pipeline 集成（事件驱动）
在 nanobot 的数据处理管道中嵌入治理检查:
- `knowledge_ingest` 后触发 `daemon.on_ingest()` 自动质检+去重+血缘
- `knowledge_search` 前触发 `daemon.on_search()` 质量门禁
- `heartbeat` 周期执行 `daemon.tick()` 持续守护
- Web 缓存写入时触发 `daemon.on_web_cache()` 记录血缘

#### 模式 C: 人工巡检（CLI / 报告）
运维人员通过 CLI 定期巡检:
- `dg health` 查看健康报告
- `dg validate` 校验数据质量
- `dg dedup` 查看重复情况（审查后加 `--execute` 删除）
- `dg alerts` 查看告警

## 与企业级方案的对比

| 特性 | 本框架 | OpenMetadata | Great Expectations |
|------|--------|-------------|-------------------|
| 部署复杂度 | pip install | Docker + MySQL + ES | pip install |
| 适用场景 | 个人 AI 助手 | 企业数据平台 | 数据管道 |
| Agent 集成 | 原生支持 | 需定制 | 需定制 |
| ChromaDB 支持 | 原生支持 | 不支持 | 不支持 |
| 中文支持 | 原生 | 有限 | 无 |
| 资源占用 | < 50MB | > 2GB | < 200MB |

本框架借鉴了上述项目的设计理念（声明式规则、数据画像、血缘追踪），但针对 Agent + 知识库场景做了轻量化适配。
