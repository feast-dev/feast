function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

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