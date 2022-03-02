import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useContext, useEffect, useRef, useState } from 'react';
import isEqual from 'lodash/isEqual';
import { EuiSystemContext, EuiThemeContext, EuiModificationsContext, EuiColorModeContext } from './context';
import { buildTheme, getColorMode, getComputed, mergeDeep } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiThemeProvider = function EuiThemeProvider(_ref) {
  var _system = _ref.theme,
      _colorMode = _ref.colorMode,
      _modifications = _ref.modify,
      children = _ref.children;
  var parentSystem = useContext(EuiSystemContext);
  var parentModifications = useContext(EuiModificationsContext);
  var parentColorMode = useContext(EuiColorModeContext);
  var parentTheme = useContext(EuiThemeContext);

  var _useState = useState(_system || parentSystem),
      _useState2 = _slicedToArray(_useState, 2),
      system = _useState2[0],
      setSystem = _useState2[1];

  var prevSystemKey = useRef(system.key);

  var _useState3 = useState(mergeDeep(parentModifications, _modifications)),
      _useState4 = _slicedToArray(_useState3, 2),
      modifications = _useState4[0],
      setModifications = _useState4[1];

  var prevModifications = useRef(modifications);

  var _useState5 = useState(getColorMode(_colorMode, parentColorMode)),
      _useState6 = _slicedToArray(_useState5, 2),
      colorMode = _useState6[0],
      setColorMode = _useState6[1];

  var prevColorMode = useRef(colorMode);
  var isParentTheme = useRef(prevSystemKey.current === parentSystem.key && colorMode === parentColorMode && isEqual(parentModifications, modifications));

  var _useState7 = useState(isParentTheme.current && Object.keys(parentTheme).length ? parentTheme : getComputed(system, buildTheme(modifications, "_".concat(system.key)), colorMode)),
      _useState8 = _slicedToArray(_useState7, 2),
      theme = _useState8[0],
      setTheme = _useState8[1];

  useEffect(function () {
    var newSystem = _system || parentSystem;

    if (prevSystemKey.current !== newSystem.key) {
      setSystem(newSystem);
      prevSystemKey.current = newSystem.key;
      isParentTheme.current = false;
    }
  }, [_system, parentSystem]);
  useEffect(function () {
    var newModifications = mergeDeep(parentModifications, _modifications);

    if (!isEqual(prevModifications.current, newModifications)) {
      setModifications(newModifications);
      prevModifications.current = newModifications;
      isParentTheme.current = false;
    }
  }, [_modifications, parentModifications]);
  useEffect(function () {
    var newColorMode = getColorMode(_colorMode, parentColorMode);

    if (!isEqual(newColorMode, prevColorMode.current)) {
      setColorMode(newColorMode);
      prevColorMode.current = newColorMode;
      isParentTheme.current = false;
    }
  }, [_colorMode, parentColorMode]);
  useEffect(function () {
    if (!isParentTheme.current) {
      setTheme(getComputed(system, buildTheme(modifications, "_".concat(system.key)), colorMode));
    }
  }, [colorMode, system, modifications]);
  return ___EmotionJSX(EuiColorModeContext.Provider, {
    value: colorMode
  }, ___EmotionJSX(EuiSystemContext.Provider, {
    value: system
  }, ___EmotionJSX(EuiModificationsContext.Provider, {
    value: modifications
  }, ___EmotionJSX(EuiThemeContext.Provider, {
    value: theme
  }, children))));
};