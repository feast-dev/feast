"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createFilter = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireDefault(require("react"));

var _is_filter = require("./is_filter");

var _field_value_selection_filter = require("./field_value_selection_filter");

var _field_value_toggle_filter = require("./field_value_toggle_filter");

var _field_value_toggle_group_filter = require("./field_value_toggle_group_filter");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var createFilter = function createFilter(index, config, query, onChange) {
  var props = {
    index: index,
    query: query,
    onChange: onChange
  }; // We don't put `config` into `props` above because until we check
  // `config.type`, TS only knows that it's a `FilterConfig`, and that type
  // is used to define `props` as well. Once we've checked `config.type`
  // below, its type is narrowed correctly, hence we pass down `config`
  // separately.

  switch (config.type) {
    case 'is':
      return (0, _react2.jsx)(_is_filter.IsFilter, (0, _extends2.default)({}, props, {
        config: config
      }));

    case 'field_value_selection':
      return (0, _react2.jsx)(_field_value_selection_filter.FieldValueSelectionFilter, (0, _extends2.default)({}, props, {
        config: config
      }));

    case 'field_value_toggle':
      return (0, _react2.jsx)(_field_value_toggle_filter.FieldValueToggleFilter, (0, _extends2.default)({}, props, {
        config: config
      }));

    case 'field_value_toggle_group':
      return (0, _react2.jsx)(_field_value_toggle_group_filter.FieldValueToggleGroupFilter, (0, _extends2.default)({}, props, {
        config: config
      }));

    default:
      // @ts-ignore TS knows that we've checked `config.type` exhaustively
      throw new Error("Unknown search filter type [".concat(config.type, "]"));
  }
};

exports.createFilter = createFilter;