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