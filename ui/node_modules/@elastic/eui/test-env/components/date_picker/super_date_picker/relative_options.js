"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.relativeUnitsFromLargestToSmallest = exports.relativeOptions = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var relativeOptions = [{
  text: 'Seconds ago',
  value: 's'
}, {
  text: 'Minutes ago',
  value: 'm'
}, {
  text: 'Hours ago',
  value: 'h'
}, {
  text: 'Days ago',
  value: 'd'
}, {
  text: 'Weeks ago',
  value: 'w'
}, {
  text: 'Months ago',
  value: 'M'
}, {
  text: 'Years ago',
  value: 'y'
}, {
  text: 'Seconds from now',
  value: 's+'
}, {
  text: 'Minutes from now',
  value: 'm+'
}, {
  text: 'Hours from now',
  value: 'h+'
}, {
  text: 'Days from now',
  value: 'd+'
}, {
  text: 'Weeks from now',
  value: 'w+'
}, {
  text: 'Months from now',
  value: 'M+'
}, {
  text: 'Years from now',
  value: 'y+'
}];
exports.relativeOptions = relativeOptions;
var timeUnitIds = relativeOptions.map(function (_ref) {
  var value = _ref.value;
  return value;
}).filter(function (value) {
  return !value.includes('+');
});
var relativeUnitsFromLargestToSmallest = timeUnitIds.reverse();
exports.relativeUnitsFromLargestToSmallest = relativeUnitsFromLargestToSmallest;