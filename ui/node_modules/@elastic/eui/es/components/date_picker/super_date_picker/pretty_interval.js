/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var MS_IN_SECOND = 1000;
var MS_IN_MINUTE = 60 * MS_IN_SECOND;
var MS_IN_HOUR = 60 * MS_IN_MINUTE;
var MS_IN_DAY = 24 * MS_IN_HOUR;
export var prettyInterval = function prettyInterval(isPaused, intervalInMs) {
  var shortHand = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
  var units;

  if (isPaused || intervalInMs === 0) {
    return 'Off';
  } else if (intervalInMs < MS_IN_MINUTE) {
    var intervalInSeconds = Math.round(intervalInMs / MS_IN_SECOND);
    if (shortHand) return "".concat(intervalInSeconds, " s");
    units = intervalInSeconds > 1 ? 'seconds' : 'second';
    return "".concat(intervalInSeconds, " ").concat(units);
  } else if (intervalInMs < MS_IN_HOUR) {
    var intervalInMinutes = Math.round(intervalInMs / MS_IN_MINUTE);
    if (shortHand) return "".concat(intervalInMinutes, " m");
    units = intervalInMinutes > 1 ? 'minutes' : 'minute';
    return "".concat(intervalInMinutes, " ").concat(units);
  } else if (intervalInMs < MS_IN_DAY) {
    var intervalInHours = Math.round(intervalInMs / MS_IN_HOUR);
    if (shortHand) return "".concat(intervalInHours, " h");
    units = intervalInHours > 1 ? 'hours' : 'hour';
    return "".concat(intervalInHours, " ").concat(units);
  }

  var intervalInDays = Math.round(intervalInMs / MS_IN_DAY);
  if (shortHand) return "".concat(intervalInDays, " d");
  units = intervalInDays > 1 ? 'days' : 'day';
  return "".concat(intervalInDays, " ").concat(units);
};