/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import dateMath from '@elastic/datemath';
import moment from 'moment'; // eslint-disable-line import/named

import { timeUnits, timeUnitsPlural } from './time_units';
import { getDateMode, DATE_MODES } from './date_modes';
import { parseRelativeParts } from './relative_utils';
var ISO_FORMAT = 'YYYY-MM-DDTHH:mm:ss.SSSZ';
export var commonDurationRanges = [{
  start: 'now/d',
  end: 'now/d',
  label: 'Today'
}, {
  start: 'now/w',
  end: 'now/w',
  label: 'This week'
}, {
  start: 'now/M',
  end: 'now/M',
  label: 'This month'
}, {
  start: 'now/y',
  end: 'now/y',
  label: 'This year'
}, {
  start: 'now-1d/d',
  end: 'now-1d/d',
  label: 'Yesterday'
}, {
  start: 'now/w',
  end: 'now',
  label: 'Week to date'
}, {
  start: 'now/M',
  end: 'now',
  label: 'Month to date'
}, {
  start: 'now/y',
  end: 'now',
  label: 'Year to date'
}];

function cantLookup(timeFrom, timeTo, dateFormat) {
  var displayFrom = formatTimeString(timeFrom, dateFormat);
  var displayTo = formatTimeString(timeTo, dateFormat, true);
  return "".concat(displayFrom, " to ").concat(displayTo);
}

function isRelativeToNow(timeFrom, timeTo) {
  var fromDateMode = getDateMode(timeFrom);
  var toDateMode = getDateMode(timeTo);
  var isLast = fromDateMode === DATE_MODES.RELATIVE && toDateMode === DATE_MODES.NOW;
  var isNext = fromDateMode === DATE_MODES.NOW && toDateMode === DATE_MODES.RELATIVE;
  return isLast || isNext;
}

export function formatTimeString(timeString, dateFormat) {
  var roundUp = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
  var locale = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 'en';
  var timeAsMoment = moment(timeString, ISO_FORMAT, true);

  if (timeAsMoment.isValid()) {
    return timeAsMoment.locale(locale).format(dateFormat);
  }

  if (timeString === 'now') {
    return 'now';
  }

  var tryParse = dateMath.parse(timeString, {
    roundUp: roundUp
  });

  if (!moment(tryParse).isValid()) {
    return 'Invalid Date';
  }

  if (moment.isMoment(tryParse)) {
    return "~ ".concat(tryParse.locale(locale).fromNow());
  }

  return timeString;
}
export function prettyDuration(timeFrom, timeTo) {
  var quickRanges = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var dateFormat = arguments.length > 3 ? arguments[3] : undefined;
  var matchingQuickRange = quickRanges.find(function (_ref) {
    var quickFrom = _ref.start,
        quickTo = _ref.end;
    return timeFrom === quickFrom && timeTo === quickTo;
  });

  if (matchingQuickRange && matchingQuickRange.label) {
    return matchingQuickRange.label;
  }

  if (isRelativeToNow(timeFrom, timeTo)) {
    var timeTense;
    var relativeParts;

    if (getDateMode(timeTo) === DATE_MODES.NOW) {
      timeTense = 'Last';
      relativeParts = parseRelativeParts(timeFrom);
    } else {
      timeTense = 'Next';
      relativeParts = parseRelativeParts(timeTo);
    }

    var countTimeUnit = relativeParts.unit.substring(0, 1);
    var countTimeUnitFullName = relativeParts.count > 1 ? timeUnitsPlural[countTimeUnit] : timeUnits[countTimeUnit];
    var text = "".concat(timeTense, " ").concat(relativeParts.count, " ").concat(countTimeUnitFullName);

    if (relativeParts.round && relativeParts.roundUnit) {
      text += " rounded to the ".concat(timeUnits[relativeParts.roundUnit]);
    }

    return text;
  }

  return cantLookup(timeFrom, timeTo, dateFormat);
}
export function showPrettyDuration(timeFrom, timeTo) {
  var quickRanges = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var matchingQuickRange = quickRanges.find(function (_ref2) {
    var quickFrom = _ref2.start,
        quickTo = _ref2.end;
    return timeFrom === quickFrom && timeTo === quickTo;
  });

  if (matchingQuickRange) {
    return true;
  }

  return isRelativeToNow(timeFrom, timeTo);
}