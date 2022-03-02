"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.providedPopoverContents = exports.DefaultColumnFormatter = void 0;

var _react = _interopRequireDefault(require("react"));

var _text = require("../../text");

var _code = require("../../code");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var DefaultColumnFormatter = function DefaultColumnFormatter(_ref) {
  var children = _ref.children;
  return (0, _react2.jsx)(_text.EuiText, null, children);
};

exports.DefaultColumnFormatter = DefaultColumnFormatter;
var providedPopoverContents = {
  json: function json(_ref2) {
    var cellContentsElement = _ref2.cellContentsElement;
    var formattedText = cellContentsElement.innerText; // attempt to pretty-print the json

    try {
      formattedText = JSON.stringify(JSON.parse(formattedText), null, 2);
    } catch (e) {} // eslint-disable-line no-empty


    return (0, _react2.jsx)(_code.EuiCodeBlock, {
      isCopyable: true,
      transparentBackground: true,
      paddingSize: "none",
      language: "json"
    }, formattedText);
  }
};
exports.providedPopoverContents = providedPopoverContents;