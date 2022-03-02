import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useState, useEffect } from 'react';
import { isWithinBreakpoints } from '../breakpoint';
/**
 * Given the current window.innerWidth and an array of breakpoint keys,
 * this hook stores state and returns true or false if the window.innerWidth
 * falls within any of the named breakpoints.
 *
 * @param {EuiBreakpointSize[]} sizes An array of named breakpoints
 * @param {boolean} isActive Manages whether the resize handler should be active
 * @returns {boolean} Returns `true` if current breakpoint name is included in `sizes`
 */

export function useIsWithinBreakpoints(sizes) {
  var isActive = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isWithinBreakpointsValue = _useState2[0],
      setIsWithinBreakpointsValue = _useState2[1];

  useEffect(function () {
    function handleResize() {
      setIsWithinBreakpointsValue(isWithinBreakpoints(window.innerWidth, sizes));
    }

    if (isActive) {
      window.removeEventListener('resize', handleResize);
      window.addEventListener('resize', handleResize);
      handleResize();
    }

    return function () {
      return window.removeEventListener('resize', handleResize);
    };
  }, [sizes, isActive]);
  return isWithinBreakpointsValue;
}