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
import unified from 'unified';
import classNames from 'classnames';
import { EuiText } from '../text/text';
import { defaultProcessingPlugins, defaultParsingPlugins } from './plugins/markdown_default_plugins';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiMarkdownFormat = function EuiMarkdownFormat(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$parsingPluginLis = _ref.parsingPluginList,
      parsingPluginList = _ref$parsingPluginLis === void 0 ? defaultParsingPlugins : _ref$parsingPluginLis,
      _ref$processingPlugin = _ref.processingPluginList,
      processingPluginList = _ref$processingPlugin === void 0 ? defaultProcessingPlugins : _ref$processingPlugin,
      _ref$textSize = _ref.textSize,
      textSize = _ref$textSize === void 0 ? 'm' : _ref$textSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "parsingPluginList", "processingPluginList", "textSize"]);

  var processor = useMemo(function () {
    return unified().use(parsingPluginList).use(processingPluginList);
  }, [parsingPluginList, processingPluginList]);
  var result = useMemo(function () {
    try {
      var _ref2;

      var processed = processor.processSync(children); // `.result` is intentionally `unknown` (https://github.com/vfile/vfile/pull/53)
      // cast to something expected.

      return (_ref2 = processed.result) !== null && _ref2 !== void 0 ? _ref2 : processed.contents;
    } catch (e) {
      return children;
    }
  }, [children, processor]);
  var classes = classNames('euiMarkdownFormat', className);
  return ___EmotionJSX(EuiText, _extends({
    size: textSize,
    className: classes
  }, rest), result);
};
EuiMarkdownFormat.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  children: PropTypes.string.isRequired,

  /** array of unified plugins to parse content into an AST */
  parsingPluginList: PropTypes.any,

  /** array of unified plugins to convert the AST into a ReactNode */
  processingPluginList: PropTypes.any,

  /**
       * Determines the text size. Choose `relative` to control the `font-size` based on the value of a parent container.
       */
  textSize: PropTypes.oneOf(["xs", "s", "m", "relative"])
};