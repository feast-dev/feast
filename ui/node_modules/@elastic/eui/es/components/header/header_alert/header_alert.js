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
import React from 'react';
import PropTypes from "prop-types";
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
EuiHeaderAlert.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Adds a link to the alert.
       */
  action: PropTypes.node,
  date: PropTypes.node.isRequired,
  text: PropTypes.node,
  title: PropTypes.node.isRequired,

  /**
       * Accepts an `EuiBadge` that displays on the alert
       */
  badge: PropTypes.node
};