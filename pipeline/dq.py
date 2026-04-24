from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass
class DQIssue:
    issue_id: int
    stage: str
    source_file: str
    source_row_num: str
    issue_type: str
    issue_detail: str
    ls_id_raw: str


class DQCollector:
    def __init__(self, stage: str) -> None:
        self._stage = stage
        self._issues: list[DQIssue] = []
        self._next_id = 1

    def add(
        self,
        source_file: str,
        source_row_num: str | int | None,
        issue_type: str,
        issue_detail: str,
        ls_id_raw: str = "",
    ) -> None:
        row_num = "" if source_row_num is None else str(source_row_num)
        issue = DQIssue(
            issue_id=self._next_id,
            stage=self._stage,
            source_file=source_file,
            source_row_num=row_num,
            issue_type=issue_type,
            issue_detail=issue_detail,
            ls_id_raw=ls_id_raw,
        )
        self._issues.append(issue)
        self._next_id += 1

    @property
    def issues(self) -> list[DQIssue]:
        return self._issues

    def as_rows(self) -> Iterable[dict[str, str | int]]:
        for item in self._issues:
            yield {
                "issue_id": item.issue_id,
                "stage": item.stage,
                "source_file": item.source_file,
                "source_row_num": item.source_row_num,
                "issue_type": item.issue_type,
                "issue_detail": item.issue_detail,
                "ls_id_raw": item.ls_id_raw,
            }
