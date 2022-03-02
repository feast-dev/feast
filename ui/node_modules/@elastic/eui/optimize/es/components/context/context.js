/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { createContext } from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";
var I18nContext = /*#__PURE__*/createContext({});
var EuiI18nProvider = I18nContext.Provider,
    EuiI18nConsumer = I18nContext.Consumer;

var EuiContext = function EuiContext(_ref) {
  var _ref$i18n = _ref.i18n,
      i18n = _ref$i18n === void 0 ? {} : _ref$i18n,
      children = _ref.children;
  return ___EmotionJSX(EuiI18nProvider, {
    value: i18n
  }, children);
};

export { EuiContext, EuiI18nConsumer, I18nContext };