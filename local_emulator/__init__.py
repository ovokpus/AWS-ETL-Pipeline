"""Local stand-in for the Partnerize feeds pipeline.

S3 is a directory tree, Glue's ``dropDuplicates`` is a Python staging step,
and Athena is DuckDB running the same joins and time windows as the Lambda SQL.
"""

from local_emulator.pipeline import RunResult, run_pipeline

__all__ = ["RunResult", "run_pipeline"]
