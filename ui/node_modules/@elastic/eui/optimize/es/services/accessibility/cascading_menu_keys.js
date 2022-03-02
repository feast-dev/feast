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
import { ARROW_DOWN, ARROW_LEFT, ARROW_RIGHT, ARROW_UP, ESCAPE, TAB } from '../keys';
export var cascadingMenuKeys = {
  ARROW_DOWN: ARROW_DOWN,
  ARROW_LEFT: ARROW_LEFT,
  ARROW_RIGHT: ARROW_RIGHT,
  ARROW_UP: ARROW_UP,
  ESCAPE: ESCAPE,
  TAB: TAB
};