"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useEuiTextDiff = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireWildcard(require("react"));

var _textDiff = _interopRequireDefault(require("text-diff"));

var _classnames = _interopRequireDefault(require("classnames"));

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var useEuiTextDiff = function useEuiTextDiff(_ref) {
  var className = _ref.className,
      _ref$insertComponent = _ref.insertComponent,
      insertComponent = _ref$insertComponent === void 0 ? 'ins' : _ref$insertComponent,
      _ref$deleteComponent = _ref.deleteComponent,
      deleteComponent = _ref$deleteComponent === void 0 ? 'del' : _ref$deleteComponent,
      sameComponent = _ref.sameComponent,
      _ref$beforeText = _ref.beforeText,
      beforeText = _ref$beforeText === void 0 ? '' : _ref$beforeText,
      _ref$afterText = _ref.afterText,
      afterText = _ref$afterText === void 0 ? '' : _ref$afterText,
      _ref$timeout = _ref.timeout,
      timeout = _ref$timeout === void 0 ? 0.1 : _ref$timeout,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["className", "insertComponent", "deleteComponent", "sameComponent", "beforeText", "afterText", "timeout"]);
  var textDiff = (0, _react.useMemo)(function () {
    var diff = new _textDiff.default({
      timeout: timeout
    }); // options may be passed to constructor

    return diff.main(beforeText, afterText);
  }, [beforeText, afterText, timeout]); // produces diff array

  var classes = (0, _classnames.default)('euiTextDiff', className);
  var rendereredHtml = (0, _react.useMemo)(function () {
    var html = [];
    if (textDiff) for (var i = 0; i < textDiff.length; i++) {
      var Element = void 0;
      var el = textDiff[i];
      if (el[0] === 1) Element = insertComponent;else if (el[0] === -1) Element = deleteComponent;else if (sameComponent) Element = sameComponent;
      if (Element) html.push((0, _react2.jsx)(Element, {
        key: i
      }, el[1]));else html.push(el[1]);
    }
    return html;
  }, [textDiff, deleteComponent, insertComponent, sameComponent]); // produces diff array

  return [(0, _react2.jsx)("span", (0, _extends2.default)({
    className: classes
  }, rest), rendereredHtml), textDiff];
};

exports.useEuiTextDiff = useEuiTextDiff;