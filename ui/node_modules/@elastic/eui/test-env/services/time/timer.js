"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Timer = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var Timer = // In a browser this is a number, but in node it's a NodeJS.Time (a
// class). We don't care about this difference.
function Timer(callback, timeMs) {
  var _this = this;

  (0, _classCallCheck2.default)(this, Timer);
  (0, _defineProperty2.default)(this, "id", void 0);
  (0, _defineProperty2.default)(this, "callback", void 0);
  (0, _defineProperty2.default)(this, "finishTime", void 0);
  (0, _defineProperty2.default)(this, "timeRemaining", void 0);
  (0, _defineProperty2.default)(this, "pause", function () {
    clearTimeout(_this.id);
    _this.id = undefined;
    _this.timeRemaining = (_this.finishTime || 0) - Date.now();
  });
  (0, _defineProperty2.default)(this, "resume", function () {
    _this.id = setTimeout(_this.finish, _this.timeRemaining);
    _this.finishTime = Date.now() + (_this.timeRemaining || 0);
    _this.timeRemaining = undefined;
  });
  (0, _defineProperty2.default)(this, "clear", function () {
    clearTimeout(_this.id);
    _this.id = undefined;
    _this.callback = undefined;
    _this.finishTime = undefined;
    _this.timeRemaining = undefined;
  });
  (0, _defineProperty2.default)(this, "finish", function () {
    if (_this.callback) {
      _this.callback();
    }

    _this.clear();
  });
  this.id = setTimeout(this.finish, timeMs);
  this.callback = callback;
  this.finishTime = Date.now() + timeMs;
  this.timeRemaining = undefined;
};

exports.Timer = Timer;