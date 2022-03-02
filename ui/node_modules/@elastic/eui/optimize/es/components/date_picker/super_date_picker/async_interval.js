import _regeneratorRuntime from "@babel/runtime/regenerator";
import _asyncToGenerator from "@babel/runtime/helpers/asyncToGenerator";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var AsyncInterval = function AsyncInterval(_fn, refreshInterval) {
  var _this = this;

  _classCallCheck(this, AsyncInterval);

  _defineProperty(this, "timeoutId", null);

  _defineProperty(this, "isStopped", false);

  _defineProperty(this, "__pendingFn", function () {});

  _defineProperty(this, "setAsyncInterval", function (fn, milliseconds) {
    if (!_this.isStopped) {
      _this.timeoutId = window.setTimeout( /*#__PURE__*/_asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee() {
        return _regeneratorRuntime.wrap(function _callee$(_context) {
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

  _defineProperty(this, "stop", function () {
    _this.isStopped = true;

    if (_this.timeoutId !== null) {
      window.clearTimeout(_this.timeoutId);
    }
  });

  this.setAsyncInterval(_fn, refreshInterval);
};