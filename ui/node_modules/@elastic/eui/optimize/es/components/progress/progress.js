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
import { EuiI18n } from '../i18n';
import { EuiInnerText } from '../inner_text';
import { keysOf } from '../common';
import { isNil } from '../../services/predicate';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  xs: 'euiProgress--xs',
  s: 'euiProgress--s',
  m: 'euiProgress--m',
  l: 'euiProgress--l'
};
export var SIZES = keysOf(sizeToClassNameMap);
var colorToClassNameMap = {
  primary: 'euiProgress--primary',
  success: 'euiProgress--success',
  warning: 'euiProgress--warning',
  danger: 'euiProgress--danger',
  subdued: 'euiProgress--subdued',
  accent: 'euiProgress--accent',
  vis0: 'euiProgress--vis0',
  vis1: 'euiProgress--vis1',
  vis2: 'euiProgress--vis2',
  vis3: 'euiProgress--vis3',
  vis4: 'euiProgress--vis4',
  vis5: 'euiProgress--vis5',
  vis6: 'euiProgress--vis6',
  vis7: 'euiProgress--vis7',
  vis8: 'euiProgress--vis8',
  vis9: 'euiProgress--vis9'
};
export var COLORS = keysOf(colorToClassNameMap);

function isNamedColor(name) {
  return colorToClassNameMap.hasOwnProperty(name);
}

var dataColorToClassNameMap = {
  primary: 'euiProgress__data--primary',
  success: 'euiProgress__data--success',
  warning: 'euiProgress__data--warning',
  danger: 'euiProgress__data--danger',
  subdued: 'euiProgress__data--subdued',
  accent: 'euiProgress__data--accent',
  vis0: 'euiProgress__data--vis0',
  vis1: 'euiProgress__data--vis1',
  vis2: 'euiProgress__data--vis2',
  vis3: 'euiProgress__data--vis3',
  vis4: 'euiProgress__data--vis4',
  vis5: 'euiProgress__data--vis5',
  vis6: 'euiProgress__data--vis6',
  vis7: 'euiProgress__data--vis7',
  vis8: 'euiProgress__data--vis8',
  vis9: 'euiProgress__data--vis9'
};
var positionsToClassNameMap = {
  fixed: 'euiProgress--fixed',
  absolute: 'euiProgress--absolute',
  static: ''
};
export var POSITIONS = keysOf(positionsToClassNameMap);
export var EuiProgress = function EuiProgress(_ref) {
  var className = _ref.className,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'success' : _ref$color,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$position = _ref.position,
      position = _ref$position === void 0 ? 'static' : _ref$position,
      max = _ref.max,
      _ref$valueText = _ref.valueText,
      valueText = _ref$valueText === void 0 ? false : _ref$valueText,
      label = _ref.label,
      value = _ref.value,
      labelProps = _ref.labelProps,
      rest = _objectWithoutProperties(_ref, ["className", "color", "size", "position", "max", "valueText", "label", "value", "labelProps"]);

  var determinate = !isNil(max);
  var colorClass = null;
  var dataColorClass = null;
  var optionalCustomStyles = null;

  if (color) {
    if (isNamedColor(color)) {
      colorClass = colorToClassNameMap[color];
      dataColorClass = dataColorToClassNameMap[color];
    } else {
      optionalCustomStyles = {
        color: color
      };
      colorClass = 'euiProgress--customColor';
    }
  }

  var classes = classNames('euiProgress', {
    'euiProgress--indeterminate': !determinate,
    'euiProgress--native': determinate
  }, sizeToClassNameMap[size], colorClass, positionsToClassNameMap[position], className);
  var dataClasses = classNames('euiProgress__data', {
    'euiProgress__data--l': size === 'l'
  }, dataColorClass);
  var labelClasses = classNames('euiProgress__label', labelProps && labelProps.className);
  var valueRender;

  if (valueText === true) {
    // valueText is true
    valueRender = ___EmotionJSX(EuiI18n, {
      token: "euiProgress.valueText",
      default: "{value}%",
      values: {
        value: value
      }
    });
  } else if (valueText) {
    // valueText exists
    valueRender = valueText;
  } // Because of a Firefox animation issue, indeterminate progress needs to not use <progress />.
  // See https://css-tricks.com/html5-progress-element/


  if (determinate) {
    return ___EmotionJSX(Fragment, null, label || valueText ? ___EmotionJSX("div", {
      className: dataClasses
    }, label && ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX("span", _extends({
        title: innerText,
        ref: ref
      }, labelProps, {
        className: labelClasses
      }), label);
    }), valueRender && ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
      return ___EmotionJSX("span", {
        title: innerText,
        ref: ref,
        style: optionalCustomStyles,
        className: "euiProgress__valueText"
      }, valueRender);
    })) : undefined, ___EmotionJSX("progress", _extends({
      className: classes,
      style: optionalCustomStyles,
      max: max,
      value: value,
      "aria-hidden": label && valueText ? true : false
    }, rest)));
  } else {
    return ___EmotionJSX("div", _extends({
      style: optionalCustomStyles,
      className: classes
    }, rest));
  }
};