import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { useGeneratedHtmlId } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiHeaderAlert = function EuiHeaderAlert(_ref) {
  var action = _ref.action,
      className = _ref.className,
      date = _ref.date,
      text = _ref.text,
      title = _ref.title,
      badge = _ref.badge,
      rest = _objectWithoutProperties(_ref, ["action", "className", "date", "text", "title", "badge"]);

  var classes = classNames('euiHeaderAlert', className);
  var ariaId = useGeneratedHtmlId();
  return ___EmotionJSX("article", _extends({
    "aria-labelledby": "".concat(ariaId, "-title"),
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    justifyContent: "spaceBetween"
  }, ___EmotionJSX(EuiFlexItem, null, ___EmotionJSX("div", {
    className: "euiHeaderAlert__date"
  }, date)), badge && ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, badge)), ___EmotionJSX("h3", {
    id: "".concat(ariaId, "-title"),
    className: "euiHeaderAlert__title"
  }, title), ___EmotionJSX("div", {
    className: "euiHeaderAlert__text"
  }, text), action && ___EmotionJSX("div", {
    className: "euiHeaderAlert__action euiLink"
  }, action));
};