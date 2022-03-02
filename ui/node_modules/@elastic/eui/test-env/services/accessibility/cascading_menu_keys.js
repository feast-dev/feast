"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.cascadingMenuKeys = void 0;

var _keys = require("../keys");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * These keys are used for navigating cascading menu UI components.
 *
 * ARROW_DOWN: Select the next item in the list.
 * ARROW_LEFT: Show the previous menu.
 * ARROW_RIGHT: Show the next menu for the selected item.
 * ARROW_UP: Select the previous item in the list.
 * ESC: Deselect the current selection and hide the list.
 * TAB: Normal tabbing navigation is still supported.
 */
var cascadingMenuKeys = {
  ARROW_DOWN: _keys.ARROW_DOWN,
  ARROW_LEFT: _keys.ARROW_LEFT,
  ARROW_RIGHT: _keys.ARROW_RIGHT,
  ARROW_UP: _keys.ARROW_UP,
  ESCAPE: _keys.ESCAPE,
  TAB: _keys.TAB
};
exports.cascadingMenuKeys = cascadingMenuKeys;