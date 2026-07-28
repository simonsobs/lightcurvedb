"""
Commands and results for unassigned-source review actions.
"""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

ReviewOutcome = Literal["external_match", "novel", "noise"]


class ExternalMatchEvidence(BaseModel):
    """
    Server-validated catalogue evidence retained with an accepted decision.
    """

    identifier: str
    object_type: str | None = None
    ra: float | None = None
    dec: float | None = None
    separation_arcmin: float | None = Field(default=None, ge=0)
    queried_at: datetime


class CandidateDecisionCommand(BaseModel):
    """
    The input for one terminal candidate decision.
    """

    source_id: UUID
    expected_version: int = Field(ge=1)
    outcome: ReviewOutcome
    reviewer: str = Field(min_length=1, max_length=256)
    external_evidence: ExternalMatchEvidence | None = None
    canonical_source_id: UUID | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> "CandidateDecisionCommand":
        if self.outcome == "external_match":
            if self.external_evidence is None:
                raise ValueError("External matches require catalogue evidence")

            if self.canonical_source_id is None:
                raise ValueError("Accepted decisions require a canonical source")

        elif self.outcome == "novel":
            if self.external_evidence is not None:
                raise ValueError("Novel decisions cannot include catalogue evidence")

            if self.canonical_source_id is None:
                raise ValueError("Accepted decisions require a canonical source")

        elif self.external_evidence is not None or self.canonical_source_id is not None:
            raise ValueError("Noise decisions cannot include canonical-source data")

        return self


class CandidateReviewDecision(BaseModel):
    """
    The completed terminal decision.
    """

    source_id: UUID
    outcome: ReviewOutcome
    canonical_source_id: UUID | None = None


class CandidateMergeCommand(BaseModel):
    """
    The input for reparenting one candidate into another.
    """

    source_id: UUID
    target_source_id: UUID
    expected_version: int = Field(ge=1)
    reviewer: str = Field(min_length=1, max_length=256)
    reason: str | None = Field(default=None, max_length=2_000)


class CandidateMerge(BaseModel):
    """
    The completed candidate merge.
    """

    source_id: UUID
    target_source_id: UUID


def decision_metadata(command: CandidateDecisionCommand) -> dict[str, Any]:
    """
    Serialize the accepted-decision context stored with an unassigned source.
    """
    metadata: dict[str, Any] = {}

    if command.canonical_source_id is not None:
        metadata["canonical_source_id"] = str(command.canonical_source_id)

    if command.external_evidence is not None:
        metadata["external_evidence"] = command.external_evidence.model_dump(
            mode="json"
        )

    return metadata
