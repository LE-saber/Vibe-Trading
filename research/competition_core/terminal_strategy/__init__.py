"""Read-only terminal tournament strategy contracts and state engine."""

from .award_line import (
    build_award_line_forecast,
    calculate_award_line_forecast,
    forecast_award_line,
    target_asset_for_award_line,
)
from .config import (
    AllocationConfig,
    AwardLineConfig,
    DEFAULT_CONFIG_PATH,
    FunnelConfig,
    TerminalCompetitionConfig,
    TerminalModeConfig,
    TerminalScheduleConfig,
    TerminalStrategyConfig,
    TerminalStrategyConfigError,
    load_terminal_config,
    load_terminal_strategy_config,
)
from .candidate_funnel import (
    CandidateAssessment,
    CandidateBlocker,
    CandidateFunnelError,
    CandidateObservation,
    FunnelResult,
    build_candidate_funnel,
    build_terminal_candidate_funnel,
    evaluate_candidate_funnel,
)
from .allocation import (
    AllocationBlocker,
    TerminalAllocationError,
    TerminalAllocationEvaluation,
    TerminalAllocationInputs,
    TerminalAllocationPolicy,
    build_terminal_allocation_policy,
    create_terminal_allocation_policy,
    evaluate_terminal_allocation,
    plan_terminal_allocation,
)
from .models import (
    AwardLineForecast,
    AwardLinePoint,
    AwardLineSample,
    ForecastConfidence,
    LeaderboardEntry,
    ModeDecision,
    TerminalBlocker,
    TerminalMode,
    TerminalModeDecision,
    TerminalModelError,
    TerminalSnapshot,
)
from .state_machine import (
    TerminalModeStateMachine,
    calculate_terminal_mode,
    decide_terminal_mode,
    evaluate_terminal_mode,
)

__all__ = [
    "AllocationConfig", "AwardLineConfig", "AwardLineForecast", "AwardLinePoint",
    "AwardLineSample", "DEFAULT_CONFIG_PATH", "ForecastConfidence", "FunnelConfig",
    "CandidateAssessment", "CandidateBlocker", "CandidateFunnelError", "CandidateObservation",
    "FunnelResult", "LeaderboardEntry", "ModeDecision", "TerminalBlocker", "TerminalCompetitionConfig",
    "TerminalMode", "TerminalModeConfig", "TerminalModeDecision", "TerminalModeStateMachine",
    "TerminalModelError", "TerminalScheduleConfig", "TerminalSnapshot",
    "TerminalStrategyConfig", "TerminalStrategyConfigError", "build_award_line_forecast",
    "calculate_award_line_forecast", "calculate_terminal_mode", "decide_terminal_mode",
    "evaluate_terminal_mode", "forecast_award_line", "load_terminal_config",
    "load_terminal_strategy_config", "target_asset_for_award_line", "build_candidate_funnel",
    "build_terminal_candidate_funnel", "evaluate_candidate_funnel", "AllocationBlocker",
    "TerminalAllocationError", "TerminalAllocationEvaluation", "TerminalAllocationInputs",
    "TerminalAllocationPolicy", "build_terminal_allocation_policy",
    "create_terminal_allocation_policy", "evaluate_terminal_allocation", "plan_terminal_allocation",
]
