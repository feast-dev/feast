"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useDependentState = useDependentState;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _react = require("react");

var _useUpdateEffect = require("./useUpdateEffect");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function useDependentState(valueFn, deps) {
  var _useState = (0, _react.useState)(valueFn),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      state = _useState2[0],
      setState = _useState2[1]; // don't call setState on initial mount


  (0, _useUpdateEffect.useUpdateEffect)(function () {
    setState(valueFn);
  }, deps);
  return [state, setState];
}