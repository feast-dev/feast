/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var isWithinRange = function isWithinRange(min, max, value) {
  if (min === '' || max === '' || value === '') {
    return false;
  }

  var val = Number(value);
  return Number(min) <= val && val <= Number(max);
}; // 1e-6 covers up to 10,000,000,000 factored by a decimal

var EPSILON = 1e-6;
export function isEvenlyDivisibleBy(num, factor) {
  var remainder = num % factor; // due to floating point issues the remainder needs to be within a margin instead of exactly 0
  // 1 % 0.1          === 0.09999999999999995
  // 1000000000 % 0.1 === 0.09999994448884877
  // 1 % 0.05         === 0.04999999999999995
  // Compare the smaller of (remainder, factor - remainder) to EPSILON

  return Math.min(remainder, // remainder may be smallest, it is 0 in the well-formed case
  Math.abs(factor - remainder) // otherwise the positive difference between factor and remainder
  ) < EPSILON;
}