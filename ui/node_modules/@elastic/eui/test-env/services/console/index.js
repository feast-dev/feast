"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _warn_once = require("./warn_once");

Object.keys(_warn_once).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _warn_once[key];
    }
  });
});