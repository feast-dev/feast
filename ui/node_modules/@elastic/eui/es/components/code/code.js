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
import PropTypes from "prop-types";
import { highlight } from 'refractor';
import classNames from 'classnames';
import { DEFAULT_LANGUAGE, checkSupportedLanguage, getHtmlContent } from './utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCode = function EuiCode(_ref) {
  var _ref$transparentBackg = _ref.transparentBackground,
      transparentBackground = _ref$transparentBackg === void 0 ? false : _ref$transparentBackg,
      _ref$language = _ref.language,
      _language = _ref$language === void 0 ? DEFAULT_LANGUAGE : _ref$language,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["transparentBackground", "language", "children", "className"]);

  var language = useMemo(function () {
    return checkSupportedLanguage(_language);
  }, [_language]);
  var data = useMemo(function () {
    if (typeof children !== 'string') {
      return [];
    }

    return highlight(children, language);
  }, [children, language]);
  var content = useMemo(function () {
    return getHtmlContent(data, children);
  }, [data, children]);
  var classes = classNames('euiCode', {
    'euiCode--transparentBackground': transparentBackground
  }, className);
  return ___EmotionJSX("code", _extends({
    className: classes,
    "data-code-language": language
  }, rest), content);
};
EuiCode.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Sets the syntax highlighting for a specific language
       * @see [https://prismjs.com/#supported-languages](https://prismjs.com/#supported-languages) for options
       */
  language: PropTypes.string,
  transparentBackground: PropTypes.bool
};