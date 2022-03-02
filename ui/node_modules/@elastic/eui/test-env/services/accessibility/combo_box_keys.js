"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.comboBoxKeys = void 0;

var _keys = require("../keys");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * These keys are used for navigating combobox UI components.
 *
 * ARROW_UP: Select the previous item in the list.
 * ARROW_DOWN: Select the next item in the list.
 * ENTER / TAB: Complete input with the current selection.
 * ESC: Deselect the current selection and hide the list.
 */
var comboBoxKeys = {
  ARROW_DOWN: _keys.ARROW_DOWN,
  ARROW_UP: _keys.ARROW_UP,
  ENTER: _keys.ENTER,
  ESCAPE: _keys.ESCAPE,
  TAB: _keys.TAB
};
exports.comboBoxKeys = comboBoxKeys;