"""AgentCorp — Recursive Delivery OS.

A local, event-sourced multi-agent orchestrator that turns

    (git repo, natural-language PRD)

into a reviewed, tested delivery:

    PRD -> Requirement -> Repository Context -> Task DAG -> Workers
        -> Reviewer -> Integration -> Supervisor -> Report

The package is deliberately layered so each concern is swappable and testable
in isolation:

======================  =====================================================
layer                   module
======================  =====================================================
domain data             :mod:`agentcorp.models`
facts / log             :mod:`agentcorp.events`, :mod:`agentcorp.store`
graph algebra           :mod:`agentcorp.graph`
planning                :mod:`agentcorp.prd`, :mod:`agentcorp.repo`,
                        :mod:`agentcorp.planner`, :mod:`agentcorp.decomposer`
execution               :mod:`agentcorp.scheduler`, :mod:`agentcorp.worker`,
                        :mod:`agentcorp.reviewer`
policy / resilience     :mod:`agentcorp.reliability`, :mod:`agentcorp.budget`,
                        :mod:`agentcorp.chaos`, :mod:`agentcorp.runtime`
governance              :mod:`agentcorp.supervisor`
facade + interfaces     :mod:`agentcorp.engine`, :mod:`agentcorp.cli`
======================  =====================================================
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
