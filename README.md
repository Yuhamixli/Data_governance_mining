# Data Governance for AI Agents

面向 AI Agent 的数据治理框架，为 [nanobot](https://github.com/user/nanobot) 等 AI 助手提供知识库数据质量管理能力。

## 为什么需要数据治理？

AI 助手的知识库会随时间不断累积数据（文档、网页缓存、聊天记录、记忆文件），如果缺乏治理机制，将面临：

| 风险 | 影响 |
|------|------|
| **重复数据** | 搜索结果冗余，检索质量下降 |
| **过期数据** | 助手给出过时甚至错误的回答 |
| **损坏数据** | 编码错误、空内容导致搜索噪音 |
| **缺失血缘** | 无法追踪问题数据的来源 |
| **无质量度量** | 数据质量持续恶化却无法察觉 |

本框架提供**自动化、可声明、Agent 可调用**的数据治理能力。

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                    Agent API Layer                        │
│  GovernanceToolkit (nanobot tools) │ CLI │ GovernanceFacade│
├─────────────────────────────────────────────────────────┤
│                    Governance Engine                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐ │
│  │ Profiler │ │  Dedup   │ │Validation│ │ Freshness  │ │
│  │ 质量画像  │ │  去重引擎 │ │ 校验框架  │ │ 新鲜度追踪 │ │
│  └──────────┘ └──────────┘ └──────────┘ └────────────┘ │
│  ┌──────────┐ ┌──────────┐                              │
│  │ Lineage  │ │ Reporter │                              │
│  │ 数据血缘  │ │ 健康报告  │                              │
│  └──────────┘ └──────────┘                              │
├─────────────────────────────────────────────────────────┤
│                    Data Sources                           │
│  ChromaDB │ Knowledge Files │ Chat History │ Memory       │
└─────────────────────────────────────────────────────────┘
```

## 核心能力

### 1. 数据质量画像 (Profiler)
- **文档级**: 完整性、有效性、新鲜度评分
- **Chunk 级**: 空内容、过短/过长、乱码、重复检测
- **集合级**: 跨文档/向量库的一致性评估

### 2. 去重引擎 (Dedup)
- **精确去重**: xxHash 内容哈希，O(n) 复杂度
- **语义去重**: Embedding 余弦相似度，发现语义等价的近似重复
- **智能保留**: 保留元数据最丰富的副本

### 3. 校验框架 (Validation)
- **声明式规则**: 内置知识库 Chunk、文档、聊天记录、记忆文件规则集
- **自定义扩展**: 通过 `ValidationRule.with_check()` 添加自定义校验
- **批量执行**: 支持 ChromaDB 集合直接校验

### 4. 新鲜度追踪 (Freshness)
- **分类策略**: 长期知识/短期知识/Web缓存/聊天记录各有不同策略
- **自动检测**: 过期/陈旧数据识别与推荐操作
- **历史趋势**: 新鲜度状态持久化，支持趋势分析

### 5. 数据血缘 (Lineage)
- **摄入追踪**: 记录文档 → 分块 → 嵌入 → 存储的完整链路
- **影响分析**: 上游源文件变更时，定位受影响的下游数据
- **可视化**: 导出 Mermaid 图表

### 6. 健康报告 (Reporter)
- **综合评分**: 加权聚合质量、校验、唯一性、新鲜度
- **告警系统**: 阈值触发告警，支持确认和解决
- **趋势追踪**: JSONL 历史记录，支持健康度变化分析

## 安装

```bash
# 从源码安装
pip install -e .

# 安装开发依赖
pip install -e ".[dev]"

# 安装全部依赖（含语义去重）
pip install -e ".[all]"
```

## 快速开始

### Python API

```python
from data_governance.api import GovernanceFacade

# 初始化（指向 nanobot 的 workspace）
gov = GovernanceFacade(
    workspace_path="~/.nanobot/workspace",
    chromadb_path="~/.nanobot/workspace/knowledge_db",
    collection_name="nanobot_kb",
)

# 综合健康检查
health = gov.health_check()
print(health.to_markdown())

# 查找重复
dedup_report = gov.find_duplicates()
print(dedup_report.summary())

# 执行去重（会实际删除重复 chunk）
# dedup_report = gov.remove_duplicates()

# 校验知识库
results = gov.validate_knowledge_base()
print(gov.validation_engine.summary(results))

# 检查新鲜度
stale = gov.get_stale_assets()
expired = gov.get_expired_assets()

# 查看告警
alerts = gov.get_alerts()
```

### CLI

```bash
# 综合健康检查
dg health -w ~/.nanobot/workspace -c ~/.nanobot/workspace/knowledge_db

# 文档质量画像
dg profile -w ~/.nanobot/workspace

# 查找重复（dry run）
dg dedup -c ~/.nanobot/workspace/knowledge_db

# 执行去重
dg dedup -c ~/.nanobot/workspace/knowledge_db --execute

# 校验
dg validate -w ~/.nanobot/workspace -c ~/.nanobot/workspace/knowledge_db

# 新鲜度检查
dg freshness -w ~/.nanobot/workspace

# 查看告警
dg alerts -w ~/.nanobot/workspace
```

### nanobot 集成

```python
from data_governance.api.tools import GovernanceToolkit

# 创建工具包
toolkit = GovernanceToolkit(
    workspace_path=workspace_path,
    chromadb_path=chromadb_path,
)

# 获取工具定义（注册到 nanobot tool registry）
tool_definitions = toolkit.get_tool_definitions()

# Agent 调用工具
result = toolkit.execute("governance_health_check", {})
result = toolkit.execute("governance_find_duplicates", {"include_semantic": False})
result = toolkit.execute("governance_validate", {"target": "all"})
```

## Agent 可用工具

| 工具名称 | 描述 |
|---------|------|
| `governance_health_check` | 综合健康检查 |
| `governance_find_duplicates` | 查找重复 chunk |
| `governance_remove_duplicates` | 删除重复 chunk |
| `governance_validate` | 校验数据质量 |
| `governance_check_freshness` | 检查数据新鲜度 |
| `governance_profile_document` | 单文档质量画像 |
| `governance_get_alerts` | 获取治理告警 |
| `governance_get_lineage` | 查询数据血缘 |

## 项目结构

```
src/data_governance/
├── core/           # 核心模型与配置
│   ├── config.py   # GovernanceConfig（Pydantic 配置）
│   └── models.py   # DataAsset, QualityReport, LineageGraph 等
├── profiler/       # 数据质量画像
│   ├── metrics.py  # 质量度量计算（完整性、有效性、熵等）
│   ├── document.py # 文档级画像
│   ├── chunk.py    # Chunk 级画像
│   └── collection.py # 集合级画像
├── dedup/          # 去重引擎
│   ├── hash_dedup.py    # 精确哈希去重
│   ├── semantic_dedup.py # 语义相似度去重
│   └── engine.py        # 统一去重流水线
├── validation/     # 校验框架
│   ├── rules.py    # 声明式规则定义
│   ├── builtin.py  # 内置规则集
│   └── engine.py   # 校验执行引擎
├── freshness/      # 新鲜度管理
│   ├── tracker.py  # 新鲜度扫描与追踪
│   └── policies.py # 过期策略定义
├── lineage/        # 数据血缘
│   └── tracker.py  # 血缘记录与查询
├── reporter/       # 健康报告
│   ├── health.py   # 健康评分聚合
│   └── alerts.py   # 告警管理
└── api/            # 对外接口
    ├── facade.py   # GovernanceFacade（统一 API）
    ├── tools.py    # GovernanceToolkit（Agent 工具）
    └── cli.py      # CLI 命令行
```

## 设计原则

1. **轻量级**: 核心依赖极少（pydantic, xxhash, numpy, chromadb），不需要部署重量级平台
2. **Agent 原生**: 所有功能通过 `GovernanceToolkit` 暴露为 Agent 可调用的工具
3. **声明式**: 校验规则、新鲜度策略均为声明式定义，易于扩展
4. **渐进式**: 可单独使用任一模块（如仅用去重），也可通过 `GovernanceFacade` 全量运行
5. **nanobot 适配**: 直接支持 ChromaDB、JSONL 聊天记录、Markdown 记忆文件等 nanobot 数据格式

## 参考

- [OpenMetadata](https://github.com/open-metadata/OpenMetadata) — 开源统一元数据平台
- [Great Expectations](https://github.com/great-expectations/great_expectations) — 声明式数据校验
- [OpenLineage](https://github.com/OpenLineage/OpenLineage) — 数据血缘标准协议

## License

MIT
