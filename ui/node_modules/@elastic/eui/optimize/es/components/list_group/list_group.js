import _extends from "@babel/runtime/helpers/extends";
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
import { EuiListGroupItem } from './list_group_item';
import { jsx as ___EmotionJSX } from "@emotion/react";
var gutterSizeToClassNameMap = {
  none: '',
  s: 'euiListGroup--gutterSmall',
  m: 'euiListGroup--gutterMedium'
};
export var GUTTER_SIZES = Object.keys(gutterSizeToClassNameMap);
export var EuiListGroup = function EuiListGroup(_ref) {
  var children = _ref.children,
      className = _ref.className,
      listItems = _ref.listItems,
      style = _ref.style,
      _ref$flush = _ref.flush,
      flush = _ref$flush === void 0 ? false : _ref$flush,
      _ref$bordered = _ref.bordered,
      bordered = _ref$bordered === void 0 ? false : _ref$bordered,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 's' : _ref$gutterSize,
      _ref$wrapText = _ref.wrapText,
      wrapText = _ref$wrapText === void 0 ? false : _ref$wrapText,
      _ref$maxWidth = _ref.maxWidth,
      maxWidth = _ref$maxWidth === void 0 ? true : _ref$maxWidth,
      _ref$showToolTips = _ref.showToolTips,
      showToolTips = _ref$showToolTips === void 0 ? false : _ref$showToolTips,
      color = _ref.color,
      size = _ref.size,
      ariaLabelledby = _ref.ariaLabelledby,
      rest = _objectWithoutProperties(_ref, ["children", "className", "listItems", "style", "flush", "bordered", "gutterSize", "wrapText", "maxWidth", "showToolTips", "color", "size", "ariaLabelledby"]);

  var newStyle;
  var widthClassName;

  if (maxWidth !== true) {
    var value;

    if (typeof maxWidth === 'number') {
      value = "".concat(maxWidth, "px");
    } else {
      value = typeof maxWidth === 'string' ? maxWidth : undefined;
    }

    newStyle = _objectSpread(_objectSpread({}, style), {}, {
      maxWidth: value
    });
  } else if (maxWidth === true) {
    widthClassName = 'euiListGroup-maxWidthDefault';
  }

  var classes = classNames('euiListGroup', {
    'euiListGroup-flush': flush,
    'euiListGroup-bordered': bordered
  }, gutterSizeToClassNameMap[gutterSize], widthClassName, className);
  var childrenOrListItems = null;

  if (listItems) {
    childrenOrListItems = listItems.map(function (item, index) {
      return [___EmotionJSX(EuiListGroupItem, _extends({
        key: "title-".concat(index),
        showToolTip: showToolTips,
        wrapText: wrapText,
        color: color,
        size: size
      }, item))];
    });
  } else {
    if (showToolTips) {
      childrenOrListItems = React.Children.map(children, function (child) {
        if ( /*#__PURE__*/React.isValidElement(child)) {
          return /*#__PURE__*/React.cloneElement(child, {
            showToolTip: true
          });
        }
      });
    } else {
      childrenOrListItems = children;
    }
  }

  return ___EmotionJSX("ul", _extends({
    className: classes,
    style: newStyle || style,
    "aria-labelledby": ariaLabelledby
  }, rest), childrenOrListItems);
};