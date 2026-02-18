"""CLI interface for data governance operations.

Usage:
    dg health --workspace /path/to/workspace --chromadb /path/to/knowledge_db
    dg profile --workspace /path/to/workspace
    dg dedup --chromadb /path/to/knowledge_db
    dg validate --workspace /path/to/workspace
    dg freshness --workspace /path/to/workspace
    dg alerts --workspace /path/to/workspace
"""

from __future__ import annotations

import sys

import click

from data_governance.api.facade import GovernanceFacade


def _create_facade(workspace: str, chromadb: str | None, collection: str) -> GovernanceFacade:
    return GovernanceFacade(
        workspace_path=workspace,
        chromadb_path=chromadb,
        collection_name=collection,
    )


@click.group()
@click.version_option(version="0.1.0", prog_name="data-governance")
def main():
    """Data Governance CLI — agent-oriented data quality management."""
    pass


@main.command()
@click.option("--workspace", "-w", required=True, help="Path to the workspace directory")
@click.option("--chromadb", "-c", default=None, help="Path to ChromaDB persist directory")
@click.option("--collection", default="xnobot_kb", help="ChromaDB collection name")
@click.option("--output", "-o", default=None, help="Save report to file")
def health(workspace: str, chromadb: str | None, collection: str, output: str | None):
    """Run comprehensive health check on the knowledge base."""
    facade = _create_facade(workspace, chromadb, collection)
    score = facade.health_check()

    report = score.to_markdown()
    click.echo(report)

    if output:
        facade.health_reporter.generate_report(score, output)
        click.echo(f"\nReport saved to: {output}")


@main.command()
@click.option("--workspace", "-w", required=True, help="Path to the workspace directory")
@click.option("--chromadb", "-c", default=None, help="Path to ChromaDB persist directory")
@click.option("--collection", default="xnobot_kb", help="ChromaDB collection name")
def profile(workspace: str, chromadb: str | None, collection: str):
    """Profile knowledge base quality."""
    facade = _create_facade(workspace, chromadb, collection)
    report = facade.profile_knowledge_base()
    click.echo(report.to_summary())


@main.command()
@click.option("--chromadb", "-c", required=True, help="Path to ChromaDB persist directory")
@click.option("--collection", default="xnobot_kb", help="ChromaDB collection name")
@click.option("--semantic", is_flag=True, default=False, help="Include semantic dedup")
@click.option("--execute", is_flag=True, default=False, help="Actually remove duplicates (default: dry run)")
def dedup(chromadb: str, collection: str, semantic: bool, execute: bool):
    """Find and optionally remove duplicates."""
    facade = _create_facade(".", chromadb, collection)

    if execute:
        click.confirm(
            "This will permanently delete duplicate chunks. Continue?", abort=True
        )
        report = facade.remove_duplicates(include_semantic=semantic)
    else:
        report = facade.find_duplicates(include_semantic=semantic)

    click.echo(report.summary())
    if not execute and report.total_duplicates > 0:
        click.echo("\nRun with --execute to remove duplicates.")


@main.command()
@click.option("--workspace", "-w", required=True, help="Path to the workspace directory")
@click.option("--chromadb", "-c", default=None, help="Path to ChromaDB persist directory")
@click.option("--collection", default="xnobot_kb", help="ChromaDB collection name")
@click.option(
    "--target",
    "-t",
    default="all",
    type=click.Choice(["all", "knowledge_base", "documents", "chat_history", "memory"]),
    help="What to validate",
)
def validate(workspace: str, chromadb: str | None, collection: str, target: str):
    """Run validation checks on data assets."""
    facade = _create_facade(workspace, chromadb, collection)

    if target == "all":
        results = []
        if chromadb:
            results.extend(facade.validate_knowledge_base())
        results.extend(facade.validate_documents())
        results.extend(facade.validate_chat_history())
        results.extend(facade.validate_memory())
    elif target == "knowledge_base":
        results = facade.validate_knowledge_base()
    elif target == "documents":
        results = facade.validate_documents()
    elif target == "chat_history":
        results = facade.validate_chat_history()
    elif target == "memory":
        results = facade.validate_memory()
    else:
        results = []

    click.echo(facade.validation_engine.summary(results))


@main.command()
@click.option("--workspace", "-w", required=True, help="Path to the workspace directory")
def freshness(workspace: str):
    """Check data freshness and identify stale/expired assets."""
    facade = _create_facade(workspace, None, "")
    report = facade.check_freshness()
    click.echo(report.summary())


@main.command()
@click.option("--workspace", "-w", required=True, help="Path to the workspace directory")
def alerts(workspace: str):
    """Show active governance alerts."""
    facade = _create_facade(workspace, None, "")
    click.echo(facade.alert_manager.summary())


@main.command(name="profile-doc")
@click.argument("file_path")
def profile_doc(file_path: str):
    """Profile a single document's quality."""
    facade = _create_facade(".", None, "")
    report = facade.profile_document(file_path)
    click.echo(report.to_summary())


if __name__ == "__main__":
    main()
