"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _number = require("./number");

Object.keys(_number).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _number[key];
    }
  });
});