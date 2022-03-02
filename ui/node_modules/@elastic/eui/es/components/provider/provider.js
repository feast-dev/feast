/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { CacheProvider } from '@emotion/react';
import { EuiGlobalStyles } from '../../global_styling';
import { EuiThemeProvider } from '../../services';
import { EuiThemeAmsterdam } from '../../themes';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiProvider = function EuiProvider(_ref) {
  var cache = _ref.cache,
      _ref$theme = _ref.theme,
      theme = _ref$theme === void 0 ? EuiThemeAmsterdam : _ref$theme,
      _ref$globalStyles = _ref.globalStyles,
      GlobalStyles = _ref$globalStyles === void 0 ? EuiGlobalStyles : _ref$globalStyles,
      colorMode = _ref.colorMode,
      modify = _ref.modify,
      children = _ref.children;
  return theme !== null && GlobalStyles !== false ? ___EmotionJSX(EuiThemeProvider, {
    theme: theme,
    colorMode: colorMode,
    modify: modify
  }, cache ? ___EmotionJSX(CacheProvider, {
    value: cache
  }, ___EmotionJSX(GlobalStyles, null)) : ___EmotionJSX(GlobalStyles, null), children) : ___EmotionJSX(EuiThemeProvider, {
    colorMode: colorMode
  }, children);
};