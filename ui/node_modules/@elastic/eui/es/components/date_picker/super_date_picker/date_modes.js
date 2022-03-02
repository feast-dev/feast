/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import dateMath from '@elastic/datemath';
import { parseRelativeParts, toRelativeStringFromParts } from './relative_utils';
import moment from 'moment';
export var DATE_MODES = {
  ABSOLUTE: 'absolute',
  RELATIVE: 'relative',
  NOW: 'now'
};
export var INVALID_DATE = 'invalid_date';
export function getDateMode(value) {
  if (value === 'now') {
    return DATE_MODES.NOW;
  }

  if (value.includes('now')) {
    return DATE_MODES.RELATIVE;
  }

  return DATE_MODES.ABSOLUTE;
}
export function toAbsoluteString(value) {
  var roundUp = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  var valueAsMoment = dateMath.parse(value, {
    roundUp: roundUp
  });

  if (!valueAsMoment) {
    return value;
  }

  if (!moment(valueAsMoment).isValid()) {
    return INVALID_DATE;
  }

  return valueAsMoment.toISOString();
}
export function toRelativeString(value) {
  return toRelativeStringFromParts(parseRelativeParts(value));
}