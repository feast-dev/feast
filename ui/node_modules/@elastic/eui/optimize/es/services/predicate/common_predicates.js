/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import moment from 'moment';
export var always = function always(_value) {
  return true;
};
export var never = function never(_value) {
  return false;
};
export var isUndefined = function isUndefined(value) {
  return value === undefined;
};
export var isNull = function isNull(value) {
  return value === null;
};
export var isNil = function isNil(value) {
  return isUndefined(value) || isNull(value);
};
export var isMoment = function isMoment(value) {
  return moment.isMoment(value);
};
export var isDate = function isDate(value) {
  return moment.isDate(value);
};
export var isDateLike = function isDateLike(value) {
  return isMoment(value) || isDate(value);
};