import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useMemo } from 'react';
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