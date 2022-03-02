function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useMemo } from 'react';
import Diff from 'text-diff';
import classNames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var useEuiTextDiff = function useEuiTextDiff(_ref) {
  var className = _ref.className,
      _ref$insertComponent = _ref.insertComponent,
      insertComponent = _ref$insertComponent === void 0 ? 'ins' : _ref$insertComponent,
      _ref$deleteComponent = _ref.deleteComponent,
      deleteComponent = _ref$deleteComponent === void 0 ? 'del' : _ref$deleteComponent,
      sameComponent = _ref.sameComponent,
      _ref$beforeText = _ref.beforeText,
      beforeText = _ref$beforeText === void 0 ? '' : _ref$beforeText,
      _ref$afterText = _ref.afterText,
      afterText = _ref$afterText === void 0 ? '' : _ref$afterText,
      _ref$timeout = _ref.timeout,
      timeout = _ref$timeout === void 0 ? 0.1 : _ref$timeout,
      rest = _objectWithoutProperties(_ref, ["className", "insertComponent", "deleteComponent", "sameComponent", "beforeText", "afterText", "timeout"]);

  var textDiff = useMemo(function () {
    var diff = new Diff({
      timeout: timeout
    }); // options may be passed to constructor

    return diff.main(beforeText, afterText);
  }, [beforeText, afterText, timeout]); // produces diff array

  var classes = classNames('euiTextDiff', className);
  var rendereredHtml = useMemo(function () {
    var html = [];
    if (textDiff) for (var i = 0; i < textDiff.length; i++) {
      var Element = void 0;
      var el = textDiff[i];
      if (el[0] === 1) Element = insertComponent;else if (el[0] === -1) Element = deleteComponent;else if (sameComponent) Element = sameComponent;
      if (Element) html.push(___EmotionJSX(Element, {
        key: i
      }, el[1]));else html.push(el[1]);
    }
    return html;
  }, [textDiff, deleteComponent, insertComponent, sameComponent]); // produces diff array

  return [___EmotionJSX("span", _extends({
    className: classes
  }, rest), rendereredHtml), textDiff];
};