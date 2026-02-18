from data_governance.api.facade import GovernanceFacade


def get_toolkit(*args, **kwargs):
    """Lazy import to avoid circular dependency."""
    from data_governance.api.tools import GovernanceToolkit
    return GovernanceToolkit(*args, **kwargs)


__all__ = ["GovernanceFacade", "get_toolkit"]
