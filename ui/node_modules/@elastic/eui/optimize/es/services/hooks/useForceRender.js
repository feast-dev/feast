import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useState, useCallback } from 'react';
export var useForceRender = function useForceRender() {
  var _useState = useState(0),
      _useState2 = _slicedToArray(_useState, 2),
      setRenderCount = _useState2[1];

  return useCallback(function () {
    setRenderCount(function (x) {
      return x + 1;
    });
  }, []);
};