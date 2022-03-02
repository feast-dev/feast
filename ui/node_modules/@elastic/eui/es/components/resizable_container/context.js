/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { createContext, useContext } from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";
var EuiResizableContainerContext = /*#__PURE__*/createContext({});
export function EuiResizableContainerContextProvider(_ref) {
  var children = _ref.children,
      registry = _ref.registry;
  return ___EmotionJSX(EuiResizableContainerContext.Provider, {
    value: {
      registry: registry
    }
  }, children);
}
export var useEuiResizableContainerContext = function useEuiResizableContainerContext() {
  var context = useContext(EuiResizableContainerContext);

  if (!context.registry) {
    throw new Error('useEuiResizableContainerContext must be used within a <EuiResizableContainerContextProvider />');
  }

  return context;
};