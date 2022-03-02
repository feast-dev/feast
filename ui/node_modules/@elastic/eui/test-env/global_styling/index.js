"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _global_styles = require("./reset/global_styles");

Object.keys(_global_styles).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _global_styles[key];
    }
  });
});