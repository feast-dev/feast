"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.accessibleClickKeys = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _keys = require("../keys");

var _accessibleClickKeys;

// These keys are used to execute click actions on interactive elements like buttons and links.
var accessibleClickKeys = (_accessibleClickKeys = {}, (0, _defineProperty2.default)(_accessibleClickKeys, _keys.ENTER, 'enter'), (0, _defineProperty2.default)(_accessibleClickKeys, _keys.SPACE, 'space'), _accessibleClickKeys);
exports.accessibleClickKeys = accessibleClickKeys;