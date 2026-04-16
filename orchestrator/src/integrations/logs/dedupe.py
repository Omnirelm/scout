"""
Log deduplication by template (e.g. Drain3).
"""
import logging
from typing import Dict, List, Any

from .base import LogEntry, DedupedLogEntry, DedupedLogsResult, LogDedupeError

logger = logging.getLogger(__name__)


def de_dupe_logs(
    logs: List[LogEntry],
    template_miner: Any,
) -> DedupedLogsResult:
    """
    De-duplicate log entries based on their message content using a template miner.

    Args:
        logs: List of LogEntry objects
        template_miner: Miner that provides add_log_message(log_message) returning dict with "template_mined" key

    Returns:
        DedupedLogsResult with entries: one DedupedLogEntry per unique template
        (representative log with normalized message, count of log lines, traces).

    Raises:
        LogDedupeError: If template_miner is None or deduplication fails.
    """
    if template_miner is None:
        raise LogDedupeError("Template miner not set. Cannot de-dupe logs.")
    template_to_traces: Dict[str, set] = {}
    template_count: Dict[str, int] = {}
    template_first_log: Dict[str, LogEntry] = {}
    for log in logs:
        msg: str = log.message
        trace_id = log.traceId or "UNKNOWN"

        if not msg or msg.strip() == "":
            continue

        result = template_miner.add_log_message(log_message=msg.strip())
        template = result.get("template_mined")

        if not template:
            logger.warning(f"Failed to extract template for message: {msg[:100]}...")
            continue

        template_to_traces.setdefault(template, set()).add(trace_id)
        template_count[template] = template_count.get(template, 0) + 1
        if template not in template_first_log:
            template_first_log[template] = log

    entries: List[DedupedLogEntry] = []
    for template, trace_ids in template_to_traces.items():
        first_log = template_first_log[template]
        log_entry_normalized = first_log.model_copy(update={"message": template})
        entries.append(
            DedupedLogEntry(
                log_entry=log_entry_normalized,
                count=template_count[template],
                traces=trace_ids,
            )
        )
    logger.info(f"De-duplicated {len(logs)} logs to {len(entries)} unique templates.")
    logger.debug(f"Sample templates: {list(template_to_traces.keys())}")
    return DedupedLogsResult(entries=entries)
