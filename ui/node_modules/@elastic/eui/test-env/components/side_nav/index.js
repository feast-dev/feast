"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  EuiSideNav: true
};
Object.defineProperty(exports, "EuiSideNav", {
  enumerable: true,
  get: function get() {
    return _side_nav.EuiSideNav;
  }
});

var _side_nav = require("./side_nav");

var _side_nav_types = require("./side_nav_types");

Object.keys(_side_nav_types).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _side_nav_types[key];
    }
  });
});