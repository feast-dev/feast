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
from datetime import datetime, timedelta
from typing import Callable, Optional


def wait_retry_backoff(
    retry_fn: Callable[[], bool],
    timeout_secs: Optional[int] = None,
    timeout_msg: Optional[str] = "Timeout while waiting for retry_fn() to return True",
    max_wait_secs: Optional[int] = None,
):
    """
    Repeated try calling given go_func until it returns True.
    Waits with a exponentiall backoff between retries
    Args:
        retry_fn: Callable that returns a boolean to retry.
        timeout_secs: timeout in seconds to give up retrying and throw TimeoutError,
                        or None to retry perpectually.
        timeout_msg: Message to use when throwing TimeoutError.
        max_wait_secs: max wait in seconds to wait between retries.
    """
    wait_begin = time.time()
    wait_secs = 2
    elapsed_secs = 0
    while not retry_fn() and elapsed_secs <= timeout_secs:
        # back off wait duration exponentially, capped at MAX_WAIT_INTERVAL_SEC
        time.sleep(wait_secs)
        wait_secs = min(wait_secs * 2, max_wait_secs)
        elapsed_secs = time.time() - wait_begin

    if elapsed_secs > timeout_secs:
        raise TimeoutError(timeout_msg)
