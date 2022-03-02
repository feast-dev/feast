"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.keys = exports.HOME = exports.END = exports.PAGE_DOWN = exports.PAGE_UP = exports.ARROW_RIGHT = exports.ARROW_LEFT = exports.ARROW_UP = exports.ARROW_DOWN = exports.F2 = exports.BACKSPACE = exports.TAB = exports.ESCAPE = exports.SPACE = exports.ENTER = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var ENTER = 'Enter';
exports.ENTER = ENTER;
var SPACE = ' ';
exports.SPACE = SPACE;
var ESCAPE = 'Escape';
exports.ESCAPE = ESCAPE;
var TAB = 'Tab';
exports.TAB = TAB;
var BACKSPACE = 'Backspace';
exports.BACKSPACE = BACKSPACE;
var F2 = 'F2';
exports.F2 = F2;
var ARROW_DOWN = 'ArrowDown';
exports.ARROW_DOWN = ARROW_DOWN;
var ARROW_UP = 'ArrowUp';
exports.ARROW_UP = ARROW_UP;
var ARROW_LEFT = 'ArrowLeft';
exports.ARROW_LEFT = ARROW_LEFT;
var ARROW_RIGHT = 'ArrowRight';
exports.ARROW_RIGHT = ARROW_RIGHT;
var PAGE_UP = 'PageUp';
exports.PAGE_UP = PAGE_UP;
var PAGE_DOWN = 'PageDown';
exports.PAGE_DOWN = PAGE_DOWN;
var END = 'End';
exports.END = END;
var HOME = 'Home';
exports.HOME = HOME;
var keys;
exports.keys = keys;

(function (keys) {
  keys["ENTER"] = "Enter";
  keys["SPACE"] = " ";
  keys["ESCAPE"] = "Escape";
  keys["TAB"] = "Tab";
  keys["BACKSPACE"] = "Backspace";
  keys["F2"] = "F2";
  keys["ARROW_DOWN"] = "ArrowDown";
  keys["ARROW_UP"] = "ArrowUp";
  keys["ARROW_LEFT"] = "ArrowLeft";
  keys["ARROW_RIGHT"] = "ArrowRight";
  keys["PAGE_UP"] = "PageUp";
  keys["PAGE_DOWN"] = "PageDown";
  keys["END"] = "End";
  keys["HOME"] = "Home";
})(keys || (exports.keys = keys = {}));