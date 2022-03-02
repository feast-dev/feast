"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.keysOf = keysOf;
exports.assertNever = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var assertNever = function assertNever(x) {
  throw new Error("Unexpected value ".concat(x));
}; // utility types:

/**
 * XOR for some properties applied to a type
 * (XOR is one of these but not both or neither)
 *
 * Usage: OneOf<typeToExtend, one | but | not | multiple | of | these | are | required>
 *
 * To require aria-label or aria-labelledby but not both
 * Example: OneOf<Type, 'aria-label' | 'aria-labelledby'>
 */


exports.assertNever = assertNever;

/**
 * Wraps Object.keys with proper typescript definition of the resulting array
 */
function keysOf(obj) {
  return Object.keys(obj);
}
/**
 * Like `keyof typeof`, but for getting values instead of keys
 * ValueOf<typeof {key1: 'value1', key2: 'value2'}>
 * Results in `'value1' | 'value2'`
 */