"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getDurationAndPerformOnFrame = exports.performOnFrame = exports.getWaitDuration = exports.getTransitionTimings = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var GROUP_NUMERIC = /^([\d.]+)(s|ms)/;

function getMilliseconds(value, unit) {
  // Given the regex match and capture groups, we can assume `unit` to be either 's' or 'ms'
  var multiplier = unit === 's' ? 1000 : 1;
  return parseFloat(value) * multiplier;
} // Find CSS `transition-duration` and `transition-delay` intervals
// and return the value of each computed property in 'ms'


var getTransitionTimings = function getTransitionTimings(element) {
  var computedStyle = window.getComputedStyle(element);
  var computedDuration = computedStyle.getPropertyValue('transition-duration');
  var durationMatchArray = computedDuration.match(GROUP_NUMERIC);
  var durationMatch = durationMatchArray ? getMilliseconds(durationMatchArray[1], durationMatchArray[2]) : 0;
  var computedDelay = computedStyle.getPropertyValue('transition-delay');
  var delayMatchArray = computedDelay.match(GROUP_NUMERIC);
  var delayMatch = delayMatchArray ? getMilliseconds(delayMatchArray[1], delayMatchArray[2]) : 0;
  return {
    durationMatch: durationMatch,
    delayMatch: delayMatch
  };
};

exports.getTransitionTimings = getTransitionTimings;

function isElementNode(element) {
  return element.nodeType === document.ELEMENT_NODE;
} // Uses `getTransitionTimings` to find the total transition time for
// all elements targeted by a MutationObserver callback


var getWaitDuration = function getWaitDuration(records) {
  return records.reduce(function (waitDuration, record) {
    // only check for CSS transition values for ELEMENT nodes
    if (isElementNode(record.target)) {
      var _getTransitionTimings = getTransitionTimings(record.target),
          durationMatch = _getTransitionTimings.durationMatch,
          delayMatch = _getTransitionTimings.delayMatch;

      waitDuration = Math.max(waitDuration, durationMatch + delayMatch);
    }

    return waitDuration;
  }, 0);
}; // Uses `requestAnimationFrame` to perform a given callback after a specified waiting period


exports.getWaitDuration = getWaitDuration;

var performOnFrame = function performOnFrame(waitDuration, toPerform) {
  if (waitDuration > 0) {
    var startTime = Date.now();
    var endTime = startTime + waitDuration;

    var onFrame = function onFrame() {
      toPerform();

      if (endTime > Date.now()) {
        requestAnimationFrame(onFrame);
      }
    };

    requestAnimationFrame(onFrame);
  }
}; // Convenience method for combining the result of 'getWaitDuration' directly with 'performOnFrame'


exports.performOnFrame = performOnFrame;

var getDurationAndPerformOnFrame = function getDurationAndPerformOnFrame(records, toPerform) {
  performOnFrame(getWaitDuration(records), toPerform);
};

exports.getDurationAndPerformOnFrame = getDurationAndPerformOnFrame;