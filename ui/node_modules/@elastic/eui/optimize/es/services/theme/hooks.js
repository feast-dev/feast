import _extends from "@babel/runtime/helpers/extends";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef, useContext } from 'react';
import { EuiThemeContext, EuiModificationsContext, EuiColorModeContext } from './context';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var useEuiTheme = function useEuiTheme() {
  var theme = useContext(EuiThemeContext);
  var colorMode = useContext(EuiColorModeContext);
  var modifications = useContext(EuiModificationsContext);
  return {
    euiTheme: theme,
    colorMode: colorMode,
    modifications: modifications
  };
};
export var withEuiTheme = function withEuiTheme(Component) {
  var componentName = Component.displayName || Component.name || 'Component';

  var Render = function Render(props, ref) {
    var _useEuiTheme = useEuiTheme(),
        euiTheme = _useEuiTheme.euiTheme,
        colorMode = _useEuiTheme.colorMode,
        modifications = _useEuiTheme.modifications;

    return ___EmotionJSX(Component, _extends({
      theme: {
        euiTheme: euiTheme,
        colorMode: colorMode,
        modifications: modifications
      },
      ref: ref
    }, props));
  };

  var WithEuiTheme = /*#__PURE__*/forwardRef(Render);
  WithEuiTheme.displayName = "WithEuiTheme(".concat(componentName, ")");
  return WithEuiTheme;
};