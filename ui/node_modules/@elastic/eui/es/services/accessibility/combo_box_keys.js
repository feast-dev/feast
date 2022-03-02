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
import { ARROW_DOWN, ENTER, ESCAPE, TAB, ARROW_UP } from '../keys';
export var comboBoxKeys = {
  ARROW_DOWN: ARROW_DOWN,
  ARROW_UP: ARROW_UP,
  ENTER: ENTER,
  ESCAPE: ESCAPE,
  TAB: TAB
};