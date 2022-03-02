import _typeof from "@babel/runtime/helpers/typeof";
import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Fragment } from 'react';
import classNames from 'classnames';
import { keysOf } from '../../common';
import { getSecureRelForTarget } from '../../../services';
import { EuiToolTip } from '../../tool_tip';
import { EuiIcon } from '../../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassMap = {
  accent: 'euiBetaBadge--accent',
  subdued: 'euiBetaBadge--subdued',
  hollow: 'euiBetaBadge--hollow'
};
export var COLORS = keysOf(colorToClassMap);
export var sizeToClassMap = {
  s: 'euiBetaBadge--small',
  m: null
};
export var SIZES = keysOf(sizeToClassMap);
export var EuiBetaBadge = function EuiBetaBadge(_ref) {
  var className = _ref.className,
      label = _ref.label,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'hollow' : _ref$color,
      tooltipContent = _ref.tooltipContent,
      _ref$tooltipPosition = _ref.tooltipPosition,
      tooltipPosition = _ref$tooltipPosition === void 0 ? 'top' : _ref$tooltipPosition,
      title = _ref.title,
      iconType = _ref.iconType,
      onClick = _ref.onClick,
      onClickAriaLabel = _ref.onClickAriaLabel,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      rest = _objectWithoutProperties(_ref, ["className", "label", "color", "tooltipContent", "tooltipPosition", "title", "iconType", "onClick", "onClickAriaLabel", "href", "rel", "target", "size"]);

  var singleLetter = false;

  if (typeof label === 'string' && label.length === 1) {
    singleLetter = true;
  }

  var classes = classNames('euiBetaBadge', {
    'euiBetaBadge--iconOnly': iconType,
    'euiBetaBadge--singleLetter': singleLetter,
    'euiBetaBadge-isClickable': onClick || href
  }, colorToClassMap[color], sizeToClassMap[size], className);
  var icon;

  if (iconType) {
    icon = ___EmotionJSX(EuiIcon, {
      className: "euiBetaBadge__icon",
      type: iconType,
      size: size === 'm' ? 'm' : 's',
      "aria-hidden": "true",
      color: "inherit" // forces the icon to inherit its parent color

    });
  }

  var Element = href ? 'a' : 'button';
  var relObj = {};

  if (href) {
    relObj.href = href;
    relObj.target = target;
    relObj.rel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
  }

  if (onClick) {
    relObj.onClick = onClick;
  }

  var content;

  if (onClick || href) {
    content = ___EmotionJSX(Element, _extends({
      "aria-label": onClickAriaLabel,
      className: classes,
      title: typeof label === 'string' ? label : title
    }, relObj, rest), icon || label);

    if (tooltipContent) {
      return ___EmotionJSX(EuiToolTip, {
        position: tooltipPosition,
        content: tooltipContent,
        title: title || label
      }, content);
    } else {
      return ___EmotionJSX(Fragment, null, content);
    }
  } else {
    if (tooltipContent) {
      return ___EmotionJSX(EuiToolTip, {
        position: tooltipPosition,
        content: tooltipContent,
        title: title || label
      }, ___EmotionJSX("span", _extends({
        tabIndex: 0,
        className: classes,
        role: "button"
      }, rest), icon || label));
    } else {
      var spanTitle = title || label;

      if (spanTitle && typeof spanTitle !== 'string') {
        console.warn("Only string titles are permitted on badges that do not use tooltips. Found: ".concat(_typeof(spanTitle)));
      }

      return ___EmotionJSX("span", _extends({
        className: classes,
        title: spanTitle
      }, rest), icon || label);
    }
  }
};