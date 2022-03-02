"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.setPropsForRestrictedPageWidth = setPropsForRestrictedPageWidth;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * The `restrictedWidth` property is the same for all EuiPage components.
 * This is file contains the type specific to that prop and a helper
 * function for creating the corresponding classNames and style tags
 * based on the consumer's configuration
 *
 * @param {restrictWidth} boolean | number | string The prop value
 * @param {style} CSSProperties An object of style attributes if provided
 * @returns {{widthClassName: string, newStyle: CSSProperties}} Returns an object with keys for the class name to append to the component's class and the updated style props
 */
function setPropsForRestrictedPageWidth(restrictWidth, style) {
  var widthClassName;
  var newStyle;

  if (restrictWidth === true) {
    widthClassName = 'restrictWidth-default';
  } else if (restrictWidth !== false) {
    widthClassName = 'restrictWidth-custom';
    newStyle = _objectSpread(_objectSpread({}, style), {}, {
      maxWidth: restrictWidth
    });
  }

  return {
    widthClassName: widthClassName,
    newStyle: newStyle
  };
}