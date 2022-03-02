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
export function timeExecution(fn) {
  var start = process.hrtime();
  fn();

  var _process$hrtime = process.hrtime(start),
      _process$hrtime2 = _slicedToArray(_process$hrtime, 2),
      seconds = _process$hrtime2[0],
      nanoseconds = _process$hrtime2[1];

  var milliseconds = seconds * 1000 + nanoseconds / 1000000;
  return milliseconds;
}
export function benchmarkFunction(fn) {
  var warmupRuns = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 3;
  var benchmarkRuns = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 3;

  // warmup v8 optimizations, cache, etc
  for (var i = 0; i < warmupRuns; i++) {
    fn();
  }

  var runTimes = [];

  for (var _i2 = 0; _i2 < benchmarkRuns; _i2++) {
    runTimes.push(timeExecution(fn));
  }

  return Math.min.apply(null, runTimes);
}