"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiResizableContainerContextProvider = EuiResizableContainerContextProvider;
exports.useEuiResizableContainerContext = void 0;

var _react = _interopRequireWildcard(require("react"));

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiResizableContainerContext = /*#__PURE__*/(0, _react.createContext)({});

function EuiResizableContainerContextProvider(_ref) {
  var children = _ref.children,
      registry = _ref.registry;
  return (0, _react2.jsx)(EuiResizableContainerContext.Provider, {
    value: {
      registry: registry
    }
  }, children);
}

var useEuiResizableContainerContext = function useEuiResizableContainerContext() {
  var context = (0, _react.useContext)(EuiResizableContainerContext);

  if (!context.registry) {
    throw new Error('useEuiResizableContainerContext must be used within a <EuiResizableContainerContextProvider />');
  }

  return context;
};

exports.useEuiResizableContainerContext = useEuiResizableContainerContext;