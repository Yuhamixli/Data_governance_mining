# nanobot 集成指南

## 安装

在 nanobot 项目中添加 data-governance 依赖:

```bash
# 方式 1: 从本地路径安装
pip install -e /path/to/Data_governance_mining

# 方式 2: 安装后作为包引用
pip install -e "A:/Projects/Data_governance_mining"
```

## 集成方案

### 方案 A: 注册为 Agent 工具

在 nanobot 的工具注册逻辑中添加治理工具:

```python
# nanobot/agent/tools/governance.py

from data_governance.api.tools import GovernanceToolkit

_toolkit: GovernanceToolkit | None = None

def _get_toolkit(workspace_path: str, chromadb_path: str) -> GovernanceToolkit:
    global _toolkit
    if _toolkit is None:
        _toolkit = GovernanceToolkit(
            workspace_path=workspace_path,
            chromadb_path=chromadb_path,
            collection_name="nanobot_kb",
        )
    return _toolkit

def get_tool_definitions(workspace_path: str, chromadb_path: str) -> list[dict]:
    """返回治理工具定义供 tool registry 注册。"""
    toolkit = _get_toolkit(workspace_path, chromadb_path)
    return toolkit.get_tool_definitions()

def execute_tool(tool_name: str, args: dict, workspace_path: str, chromadb_path: str) -> str:
    """执行治理工具。"""
    toolkit = _get_toolkit(workspace_path, chromadb_path)
    return toolkit.execute(tool_name, args)
```

### 方案 B: 在 Heartbeat 中集成

在 nanobot 的 heartbeat service 中添加定期健康检查:

```python
# 在 nanobot/heartbeat/service.py 中添加

from data_governance.api import GovernanceFacade

async def periodic_governance_check(workspace_path: str, chromadb_path: str):
    """定期执行数据治理检查。"""
    gov = GovernanceFacade(
        workspace_path=workspace_path,
        chromadb_path=chromadb_path,
    )

    # 健康检查
    health = gov.health_check()

    if health.overall < 0.6:
        # 触发告警 — 可以通知管理员
        alerts = gov.get_alerts()
        logger.warning(f"Knowledge base health low: {health.overall:.1%}")
        for alert in alerts:
            logger.warning(f"  Alert: {alert['title']}")

    # 自动去重（谨慎使用）
    dedup = gov.find_duplicates()
    if dedup.total_duplicates > 0:
        logger.info(f"Found {dedup.total_duplicates} duplicates")
        # gov.remove_duplicates()  # 取消注释以自动清理
```

### 方案 C: 在文档摄入前校验

在 knowledge_ingest 工具中添加质量门禁:

```python
# 在 knowledge ingestion 流程中添加

from data_governance.api import GovernanceFacade

def ingest_with_governance(file_path: str, workspace_path: str, chromadb_path: str):
    gov = GovernanceFacade(workspace_path=workspace_path, chromadb_path=chromadb_path)

    # 摄入前: 文档质量评估
    report = gov.profile_document(file_path)
    if report.overall_score < 0.3:
        return f"Document quality too low ({report.overall_score:.1%}), skipping ingestion"

    # 执行正常摄入...
    # ingest_document(file_path)

    # 摄入后: 去重检查
    dedup = gov.find_duplicates()
    if dedup.total_duplicates > 0:
        gov.remove_duplicates()
        return f"Ingested successfully, removed {dedup.total_duplicates} duplicates"

    return "Ingested successfully"
```

## Agent 使用场景

注册工具后，nanobot Agent 可以在以下场景自动调用治理工具:

1. **用户问 "检查知识库健康度"** → Agent 调用 `governance_health_check`
2. **摄入文档前** → Agent 先调用 `governance_profile_document` 评估
3. **发现搜索结果重复** → Agent 调用 `governance_find_duplicates`
4. **定期维护** → Heartbeat 自动调用 `governance_health_check`
5. **调试回答质量** → Agent 通过 `governance_get_lineage` 追踪数据来源

## 配置

在 nanobot 的 config.json 中可以添加治理配置:

```json
{
  "governance": {
    "profiler": {
      "min_chunk_length": 20,
      "max_chunk_length": 5000,
      "similarity_threshold": 0.95
    },
    "dedup": {
      "semantic_threshold": 0.95,
      "dry_run": false
    },
    "freshness": {
      "default_ttl_days": 30,
      "web_cache_ttl_days": 7,
      "stale_threshold_days": 180
    },
    "reporter": {
      "alert_threshold": 0.6
    }
  }
}
```
