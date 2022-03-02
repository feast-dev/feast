"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.PropertySortType = void 0;

var _propTypes = _interopRequireDefault(require("prop-types"));

var _sort_direction = require("./sort_direction");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var PropertySortType = _propTypes.default.shape({
  field: _propTypes.default.string.isRequired,
  direction: _sort_direction.SortDirectionType.isRequired
});

exports.PropertySortType = PropertySortType;