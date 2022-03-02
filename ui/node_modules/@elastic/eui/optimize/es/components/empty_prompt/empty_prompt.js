import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { keysOf } from '../common';
import { EuiTitle } from '../title';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { EuiSpacer } from '../spacer';
import { EuiIcon } from '../icon';
import { isNamedColor } from '../icon/named_colors';
import { EuiText, EuiTextColor } from '../text';
import { EuiPanel } from '../panel/panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiEmptyPrompt--paddingSmall',
  m: 'euiEmptyPrompt--paddingMedium',
  l: 'euiEmptyPrompt--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiEmptyPrompt = function EuiEmptyPrompt(_ref) {
  var icon = _ref.icon,
      iconType = _ref.iconType,
      _iconColor = _ref.iconColor,
      title = _ref.title,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 'm' : _ref$titleSize,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'l' : _ref$paddingSize,
      body = _ref.body,
      actions = _ref.actions,
      className = _ref.className,
      _ref$layout = _ref.layout,
      layout = _ref$layout === void 0 ? 'vertical' : _ref$layout,
      hasBorder = _ref.hasBorder,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'transparent' : _ref$color,
      footer = _ref.footer,
      rest = _objectWithoutProperties(_ref, ["icon", "iconType", "iconColor", "title", "titleSize", "paddingSize", "body", "actions", "className", "layout", "hasBorder", "color", "footer"]);

  var isVerticalLayout = layout === 'vertical'; // Default the iconColor to `subdued`,
  // otherwise try to match the iconColor with the panel color unless iconColor is specified

  var iconColor = _iconColor !== null && _iconColor !== void 0 ? _iconColor : isNamedColor(color) ? color : 'subdued';
  var iconNode = iconType ? ___EmotionJSX(EuiIcon, {
    type: iconType,
    size: "xxl",
    color: iconColor
  }) : icon;
  var titleNode;
  var bodyNode;

  if (body || title) {
    if (title) {
      titleNode = ___EmotionJSX(EuiTitle, {
        size: titleSize
      }, title);
    }

    if (body) {
      bodyNode = ___EmotionJSX(EuiTextColor, {
        color: "subdued"
      }, title && ___EmotionJSX(EuiSpacer, {
        size: "m"
      }), ___EmotionJSX(EuiText, null, body));
    }
  }

  var actionsNode;

  if (actions) {
    var actionsRow;

    if (Array.isArray(actions)) {
      actionsRow = ___EmotionJSX(EuiFlexGroup, {
        className: "euiEmptyPrompt__actions",
        gutterSize: "m",
        alignItems: "center",
        justifyContent: "center",
        direction: isVerticalLayout ? 'column' : 'row'
      }, actions.map(function (action, index) {
        return ___EmotionJSX(EuiFlexItem, {
          key: index,
          grow: false
        }, action);
      }));
    } else {
      actionsRow = actions;
    }

    actionsNode = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiSpacer, {
      size: "l"
    }), actionsRow);
  }

  var contentNodes = ___EmotionJSX(React.Fragment, null, titleNode, bodyNode, actionsNode);

  var classes = classNames('euiEmptyPrompt', ["euiEmptyPrompt--".concat(layout)], paddingSizeToClassNameMap[paddingSize], className);

  var panelProps = _objectSpread({
    className: classes,
    color: color,
    paddingSize: 'none',
    hasBorder: hasBorder
  }, rest);

  return ___EmotionJSX(EuiPanel, panelProps, ___EmotionJSX("div", {
    className: "euiEmptyPrompt__main"
  }, iconNode && ___EmotionJSX("div", {
    className: "euiEmptyPrompt__icon"
  }, iconNode), ___EmotionJSX("div", {
    className: "euiEmptyPrompt__content"
  }, ___EmotionJSX("div", {
    className: "euiEmptyPrompt__contentInner"
  }, contentNodes))), footer && ___EmotionJSX("div", {
    className: "euiEmptyPrompt__footer"
  }, footer));
};