import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState, useEffect } from 'react';
import { throttle } from '../../services';
import { getBreakpoint } from '../../services/breakpoint';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiShowFor = function EuiShowFor(_ref) {
  var children = _ref.children,
      sizes = _ref.sizes;

  var _useState = useState(getBreakpoint(typeof window === 'undefined' ? -Infinity : window.innerWidth)),
      _useState2 = _slicedToArray(_useState, 2),
      currentBreakpoint = _useState2[0],
      setCurrentBreakpoint = _useState2[1];

  var functionToCallOnWindowResize = throttle(function () {
    var newBreakpoint = getBreakpoint(window.innerWidth);

    if (newBreakpoint !== currentBreakpoint) {
      setCurrentBreakpoint(newBreakpoint);
    } // reacts every 50ms to resize changes and always gets the final update

  }, 50); // Add window resize handlers

  useEffect(function () {
    window.addEventListener('resize', functionToCallOnWindowResize);
    return function () {
      window.removeEventListener('resize', functionToCallOnWindowResize);
    };
  }, [sizes, functionToCallOnWindowResize]);

  if (sizes === 'all' || sizes.includes(currentBreakpoint)) {
    return ___EmotionJSX(React.Fragment, null, children);
  }

  return null;
};