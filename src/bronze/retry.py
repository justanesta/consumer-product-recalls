from __future__ import annotations

import tenacity

from src.extractors._base import RateLimitError, TransientExtractionError

# Decorator forms of the retry policies from ADR 0013.
# Apply these to concrete extractor helper methods that need retry outside the
# lifecycle template (Extractor.run() handles the lifecycle steps directly via
# Retrying.__call__()).
#
# Usage:
#   from src.bronze.retry import transient_retry, r2_retry
#
#   class CpscExtractor(RestApiExtractor[CpscRecord]):
#       @transient_retry
#       def _fetch_page(self, offset: int) -> dict: ...

transient_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type((TransientExtractionError, RateLimitError)),
    wait=tenacity.wait_exponential_jitter(initial=1, max=60),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

r2_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type(TransientExtractionError),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
