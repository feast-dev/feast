"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _prop_types = require("./prop_types");

Object.keys(_prop_types).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _prop_types[key];
    }
  });
});