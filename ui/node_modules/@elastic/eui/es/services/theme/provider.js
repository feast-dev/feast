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