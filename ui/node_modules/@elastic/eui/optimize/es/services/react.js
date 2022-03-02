/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { unstable_batchedUpdates } from 'react-dom';
var _queue = [];

function processQueue() {
  // the queued functions trigger react setStates which, if unbatched,
  // each cause a full update->render->dom pass _per function_
  // instead, tell React to wait until all updates are finished before re-rendering
  unstable_batchedUpdates(function () {
    for (var i = 0; i < _queue.length; i++) {
      _queue[i]();
    }

    _queue.length = 0;
  });
}

export function enqueueStateChange(fn) {
  if (_queue.length === 0) {
    setTimeout(processQueue);
  }

  _queue.push(fn);
}