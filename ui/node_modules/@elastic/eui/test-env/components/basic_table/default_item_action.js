"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.DefaultItemAction = void 0;

var _react = _interopRequireDefault(require("react"));

var _predicate = require("../../services/predicate");

var _button = require("../button");

var _tool_tip = require("../tool_tip");

var _accessibility = require("../../services/accessibility");

var _accessibility2 = require("../accessibility");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// In order to use generics with an arrow function inside a .tsx file, it's necessary to use
// this `extends` hack and declare the types as shown, instead of declaring the const as a
// FunctionComponent
var DefaultItemAction = function DefaultItemAction(_ref) {
  var action = _ref.action,
      enabled = _ref.enabled,
      item = _ref.item,
      className = _ref.className;

  if (!action.onClick && !action.href) {
    throw new Error("Cannot render item action [".concat(action.name, "]. Missing required 'onClick' callback\n      or 'href' string. If you want to provide a custom action control, make sure to define the 'render' callback"));
  }

  var onClick = action.onClick ? function () {
    return action.onClick(item);
  } : undefined;
  var buttonColor = action.color;
  var color = 'primary';

  if (buttonColor) {
    color = (0, _predicate.isString)(buttonColor) ? buttonColor : buttonColor(item);
  }

  var buttonIcon = action.icon;
  var icon;

  if (buttonIcon) {
    icon = (0, _predicate.isString)(buttonIcon) ? buttonIcon : buttonIcon(item);
  }

  var button;
  var actionContent = typeof action.name === 'function' ? action.name(item) : action.name;
  var ariaLabelId = (0, _accessibility.useGeneratedHtmlId)();

  if (action.type === 'icon') {
    if (!icon) {
      throw new Error("Cannot render item action [".concat(action.name, "]. It is configured to render as an icon but no\n      icon is provided. Make sure to set the 'icon' property of the action"));
    }

    button = (0, _react2.jsx)(_react.default.Fragment, null, (0, _react2.jsx)(_button.EuiButtonIcon, {
      className: className,
      "aria-labelledby": ariaLabelId,
      isDisabled: !enabled,
      color: color,
      iconType: icon,
      onClick: onClick,
      href: action.href,
      target: action.target,
      "data-test-subj": action['data-test-subj']
    }), (0, _react2.jsx)(_accessibility2.EuiScreenReaderOnly, null, (0, _react2.jsx)("span", {
      id: ariaLabelId
    }, actionContent)));
  } else {
    button = (0, _react2.jsx)(_button.EuiButtonEmpty, {
      className: className,
      size: "s",
      isDisabled: !enabled,
      color: color,
      iconType: icon,
      onClick: onClick,
      href: action.href,
      target: action.target,
      "data-test-subj": action['data-test-subj'],
      flush: "right"
    }, actionContent);
  }

  return enabled && action.description ? (0, _react2.jsx)(_tool_tip.EuiToolTip, {
    content: action.description,
    delay: "long"
  }, button) : button;
};

exports.DefaultItemAction = DefaultItemAction;