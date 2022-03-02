"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _common_predicates = require("./common_predicates");

Object.keys(_common_predicates).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _common_predicates[key];
    }
  });
});

var _lodash_predicates = require("./lodash_predicates");

Object.keys(_lodash_predicates).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _lodash_predicates[key];
    }
  });
});