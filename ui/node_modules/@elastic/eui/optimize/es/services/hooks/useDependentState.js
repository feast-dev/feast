import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useState } from 'react';
import { useUpdateEffect } from './useUpdateEffect';
export function useDependentState(valueFn, deps) {
  var _useState = useState(valueFn),
      _useState2 = _slicedToArray(_useState, 2),
      state = _useState2[0],
      setState = _useState2[1]; // don't call setState on initial mount


  useUpdateEffect(function () {
    setState(valueFn);
  }, deps);
  return [state, setState];
}