"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.AsyncInterval = void 0;

var _regenerator = _interopRequireDefault(require("@babel/runtime/regenerator"));

var _asyncToGenerator2 = _interopRequireDefault(require("@babel/runtime/helpers/asyncToGenerator"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var AsyncInterval = function AsyncInterval(_fn, refreshInterval) {
  var _this = this;

  (0, _classCallCheck2.default)(this, AsyncInterval);
  (0, _defineProperty2.default)(this, "timeoutId", null);
  (0, _defineProperty2.default)(this, "isStopped", false);
  (0, _defineProperty2.default)(this, "__pendingFn", function () {});
  (0, _defineProperty2.default)(this, "setAsyncInterval", function (fn, milliseconds) {
    if (!_this.isStopped) {
      _this.timeoutId = window.setTimeout( /*#__PURE__*/(0, _asyncToGenerator2.default)( /*#__PURE__*/_regenerator.default.mark(function _callee() {
        return _regenerator.default.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                _context.next = 2;
                return fn();

              case 2:
                _this.__pendingFn = _context.sent;

                _this.setAsyncInterval(fn, milliseconds);

              case 4:
              case "end":
                return _context.stop();
            }
          }
        }, _callee);
      })), milliseconds);
    }
  });
  (0, _defineProperty2.default)(this, "stop", function () {
    _this.isStopped = true;

    if (_this.timeoutId !== null) {
      window.clearTimeout(_this.timeoutId);
    }
  });
  this.setAsyncInterval(_fn, refreshInterval);
};

exports.AsyncInterval = AsyncInterval;