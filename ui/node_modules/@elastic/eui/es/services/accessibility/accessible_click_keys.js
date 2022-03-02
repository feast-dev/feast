var _accessibleClickKeys;

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { ENTER, SPACE } from '../keys'; // These keys are used to execute click actions on interactive elements like buttons and links.

export var accessibleClickKeys = (_accessibleClickKeys = {}, _defineProperty(_accessibleClickKeys, ENTER, 'enter'), _defineProperty(_accessibleClickKeys, SPACE, 'space'), _accessibleClickKeys);