# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from typing import Any, Callable, Optional, Tuple

from feast.constants import MAX_WAIT_INTERVAL


def wait_retry_backoff(
    retry_fn: Callable[[], Tuple[Any, bool]],
    timeout_secs: int = 0,
    timeout_msg: Optional[str] = "Timeout while waiting for retry_fn() to return True",
    max_interval_secs: int = int(MAX_WAIT_INTERVAL),
) -> Any:
    """
    Repeatedly try calling given retry_fn until it returns a True boolean success flag.
    Waits with a exponential backoff between retries until timeout when it throws TimeoutError.
    Args:
        retry_fn: Callable that returns a result and a boolean success flag.
        timeout_secs: timeout in seconds to give up retrying and throw TimeoutError,
                        or 0 to retry perpetually.
        timeout_msg: Message to use when throwing TimeoutError.
        max_interval_secs: max wait in seconds to wait between retries.
    Returns:
        Returned Result from retry_fn() if success flag is True.
    """
    wait_secs, elapsed_secs = 1.0, 0.0
    result, is_success = retry_fn()
    wait_begin = time.time()
    while not is_success and (elapsed_secs <= timeout_secs or timeout_secs == 0):
        # back off wait duration exponentially, capped at MAX_WAIT_INTERVAL_SEC
        elapsed_secs = time.time() - wait_begin
        till_timeout_secs = timeout_secs - elapsed_secs
        wait_secs = min(wait_secs * 2, max_interval_secs, till_timeout_secs)
        time.sleep(wait_secs)
        # retry call
        result, is_success = retry_fn()
        elapsed_secs = time.time() - wait_begin

    if not is_success and elapsed_secs > timeout_secs:
        raise TimeoutError(timeout_msg)

    return result
