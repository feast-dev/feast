"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.newDate = newDate;
exports.newDateWithOffset = newDateWithOffset;
exports.now = now;
exports.cloneDate = cloneDate;
exports.parseDate = parseDate;
exports.isMoment = isMoment;
exports.isDate = isDate;
exports.formatDate = formatDate;
exports.safeDateFormat = safeDateFormat;
exports.setTime = setTime;
exports.setMonth = setMonth;
exports.setYear = setYear;
exports.setUTCOffset = setUTCOffset;
exports.getMillisecond = getMillisecond;
exports.getSecond = getSecond;
exports.getMinute = getMinute;
exports.getHour = getHour;
exports.getDay = getDay;
exports.getWeek = getWeek;
exports.getMonth = getMonth;
exports.getYear = getYear;
exports.getDate = getDate;
exports.getUTCOffset = getUTCOffset;
exports.getDayOfWeekCode = getDayOfWeekCode;
exports.getStartOfDay = getStartOfDay;
exports.getStartOfWeek = getStartOfWeek;
exports.getStartOfMonth = getStartOfMonth;
exports.getStartOfDate = getStartOfDate;
exports.getEndOfWeek = getEndOfWeek;
exports.getEndOfMonth = getEndOfMonth;
exports.addMinutes = addMinutes;
exports.addHours = addHours;
exports.addDays = addDays;
exports.addWeeks = addWeeks;
exports.addMonths = addMonths;
exports.addYears = addYears;
exports.subtractDays = subtractDays;
exports.subtractWeeks = subtractWeeks;
exports.subtractMonths = subtractMonths;
exports.subtractYears = subtractYears;
exports.isBefore = isBefore;
exports.isAfter = isAfter;
exports.equals = equals;
exports.isSameYear = isSameYear;
exports.isSameMonth = isSameMonth;
exports.isSameDay = isSameDay;
exports.isSameTime = isSameTime;
exports.isSameUtcOffset = isSameUtcOffset;
exports.isDayInRange = isDayInRange;
exports.getDaysDiff = getDaysDiff;
exports.localizeDate = localizeDate;
exports.getDefaultLocale = getDefaultLocale;
exports.getDefaultLocaleData = getDefaultLocaleData;
exports.registerLocale = registerLocale;
exports.getLocaleData = getLocaleData;
exports.getLocaleDataForLocale = getLocaleDataForLocale;
exports.getFormattedWeekdayInLocale = getFormattedWeekdayInLocale;
exports.getWeekdayMinInLocale = getWeekdayMinInLocale;
exports.getWeekdayShortInLocale = getWeekdayShortInLocale;
exports.getMonthInLocale = getMonthInLocale;
exports.getMonthShortInLocale = getMonthShortInLocale;
exports.isDayDisabled = isDayDisabled;
exports.isOutOfBounds = isOutOfBounds;
exports.isTimeDisabled = isTimeDisabled;
exports.isTimeInDisabledRange = isTimeInDisabledRange;
exports.allDaysDisabledBefore = allDaysDisabledBefore;
exports.allDaysDisabledAfter = allDaysDisabledAfter;
exports.getEffectiveMinDate = getEffectiveMinDate;
exports.getEffectiveMaxDate = getEffectiveMaxDate;
exports.getHightLightDaysMap = getHightLightDaysMap;
exports.timesToInjectAfter = timesToInjectAfter;

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _moment = _interopRequireDefault(require("moment"));

/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2018 HackerOne Inc and individual contributors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 */
var dayOfWeekCodes = {
  1: "mon",
  2: "tue",
  3: "wed",
  4: "thu",
  5: "fri",
  6: "sat",
  7: "sun"
}; // These functions are not exported so
// that we avoid magic strings like 'days'

function set(date, unit, to) {
  return date.set(unit, to);
}

function add(date, amount, unit) {
  return date.add(amount, unit);
}

function subtract(date, amount, unit) {
  return date.subtract(amount, unit);
}

function get(date, unit) {
  return date.get(unit);
}

function getStartOf(date, unit) {
  return date.startOf(unit);
}

function getEndOf(date, unit) {
  return date.endOf(unit);
}

function getDiff(date1, date2, unit) {
  return date1.diff(date2, unit);
} // ** Date Constructors **


function newDate(point) {
  return (0, _moment.default)(point);
}

function newDateWithOffset(utcOffset) {
  return (0, _moment.default)().utc().utcOffset(utcOffset);
}

function now(maybeFixedUtcOffset) {
  if (maybeFixedUtcOffset == null) {
    return newDate();
  }

  return newDateWithOffset(maybeFixedUtcOffset);
}

function cloneDate(date) {
  return date.clone();
}

function parseDate(value, _ref) {
  var dateFormat = _ref.dateFormat,
      locale = _ref.locale,
      strictParsing = _ref.strictParsing;
  var m = (0, _moment.default)(value, dateFormat, locale || _moment.default.locale(), strictParsing);
  return m.isValid() ? m : null;
} // ** Date "Reflection" **


function isMoment(date) {
  return _moment.default.isMoment(date);
}

function isDate(date) {
  return _moment.default.isDate(date);
} // ** Date Formatting **


function formatDate(date, format) {
  return date.format(format);
}

function safeDateFormat(date, _ref2) {
  var dateFormat = _ref2.dateFormat,
      locale = _ref2.locale;
  return date && date.clone().locale(locale || _moment.default.locale()).format(Array.isArray(dateFormat) ? dateFormat[0] : dateFormat) || "";
} // ** Date Setters **


function setTime(date, _ref3) {
  var hour = _ref3.hour,
      minute = _ref3.minute,
      second = _ref3.second,
      millisecond = _ref3.millisecond;
  date.set({
    hour: hour,
    minute: minute,
    second: second,
    millisecond: millisecond
  });
  return date;
}

function setMonth(date, month) {
  return set(date, "month", month);
}

function setYear(date, year) {
  return set(date, "year", year);
}

function setUTCOffset(date, offset) {
  return date.utcOffset(offset);
} // ** Date Getters **


function getMillisecond(date) {
  return get(date, "millisecond");
}

function getSecond(date) {
  return get(date, "second");
}

function getMinute(date) {
  return get(date, "minute");
}

function getHour(date) {
  return get(date, "hour");
} // Returns day of week


function getDay(date) {
  return get(date, "day");
}

function getWeek(date) {
  return get(date, "week");
}

function getMonth(date) {
  return get(date, "month");
}

function getYear(date) {
  return get(date, "year");
} // Returns day of month


function getDate(date) {
  return get(date, "date");
}

function getUTCOffset() {
  return (0, _moment.default)().utcOffset();
}

function getDayOfWeekCode(day) {
  return dayOfWeekCodes[day.isoWeekday()];
} // *** Start of ***


function getStartOfDay(date) {
  return getStartOf(date, "day");
}

function getStartOfWeek(date) {
  return getStartOf(date, "week");
}

function getStartOfMonth(date) {
  return getStartOf(date, "month");
}

function getStartOfDate(date) {
  return getStartOf(date, "date");
} // *** End of ***


function getEndOfWeek(date) {
  return getEndOf(date, "week");
}

function getEndOfMonth(date) {
  return getEndOf(date, "month");
} // ** Date Math **
// *** Addition ***


function addMinutes(date, amount) {
  return add(date, amount, "minutes");
}

function addHours(date, amount) {
  return add(date, amount, "hours");
}

function addDays(date, amount) {
  return add(date, amount, "days");
}

function addWeeks(date, amount) {
  return add(date, amount, "weeks");
}

function addMonths(date, amount) {
  return add(date, amount, "months");
}

function addYears(date, amount) {
  return add(date, amount, "years");
} // *** Subtraction ***


function subtractDays(date, amount) {
  return subtract(date, amount, "days");
}

function subtractWeeks(date, amount) {
  return subtract(date, amount, "weeks");
}

function subtractMonths(date, amount) {
  return subtract(date, amount, "months");
}

function subtractYears(date, amount) {
  return subtract(date, amount, "years");
} // ** Date Comparison **


function isBefore(date1, date2) {
  return date1.isBefore(date2);
}

function isAfter(date1, date2) {
  return date1.isAfter(date2);
}

function equals(date1, date2) {
  return date1.isSame(date2);
}

function isSameYear(date1, date2) {
  if (date1 && date2) {
    return date1.isSame(date2, "year");
  } else {
    return !date1 && !date2;
  }
}

function isSameMonth(date1, date2) {
  if (date1 && date2) {
    return date1.isSame(date2, "month");
  } else {
    return !date1 && !date2;
  }
}

function isSameDay(moment1, moment2) {
  if (moment1 && moment2) {
    return moment1.isSame(moment2, "day");
  } else {
    return !moment1 && !moment2;
  }
}

function isSameTime(moment1, moment2) {
  if (moment1 && moment2) {
    return moment1.isSame(moment2, "second");
  } else {
    return !moment1 && !moment2;
  }
}

function isSameUtcOffset(moment1, moment2) {
  if (moment1 && moment2) {
    return moment1.utcOffset() === moment2.utcOffset();
  } else {
    return !moment1 && !moment2;
  }
}

function isDayInRange(day, startDate, endDate) {
  var before = startDate.clone().startOf("day").subtract(1, "seconds");
  var after = endDate.clone().startOf("day").add(1, "seconds");
  return day.clone().startOf("day").isBetween(before, after);
} // *** Diffing ***


function getDaysDiff(date1, date2) {
  return getDiff(date1, date2, "days");
} // ** Date Localization **


function localizeDate(date, locale) {
  return date.clone().locale(locale || _moment.default.locale());
}

function getDefaultLocale() {
  return _moment.default.locale();
}

function getDefaultLocaleData() {
  return _moment.default.localeData();
}

function registerLocale(localeName, localeData) {
  _moment.default.defineLocale(localeName, localeData);
}

function getLocaleData(date) {
  return date.localeData();
}

function getLocaleDataForLocale(locale) {
  return _moment.default.localeData(locale);
}

function getFormattedWeekdayInLocale(locale, date, formatFunc) {
  return formatFunc(locale.weekdays(date));
}

function getWeekdayMinInLocale(locale, date) {
  return locale.weekdaysMin(date);
}

function getWeekdayShortInLocale(locale, date) {
  return locale.weekdaysShort(date);
} // TODO what is this format exactly?


function getMonthInLocale(locale, date, format) {
  return locale.months(date, format);
}

function getMonthShortInLocale(locale, date) {
  return locale.monthsShort(date);
} // ** Utils for some components **


function isDayDisabled(day) {
  var _ref4 = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {},
      minDate = _ref4.minDate,
      maxDate = _ref4.maxDate,
      excludeDates = _ref4.excludeDates,
      includeDates = _ref4.includeDates,
      filterDate = _ref4.filterDate;

  return minDate && day.isBefore(minDate, "day") || maxDate && day.isAfter(maxDate, "day") || excludeDates && excludeDates.some(function (excludeDate) {
    return isSameDay(day, excludeDate);
  }) || includeDates && !includeDates.some(function (includeDate) {
    return isSameDay(day, includeDate);
  }) || filterDate && !filterDate(day.clone()) || false;
}

function isOutOfBounds(day) {
  var _ref5 = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {},
      minDate = _ref5.minDate,
      maxDate = _ref5.maxDate;

  return minDate && day.isBefore(minDate, "day") || maxDate && day.isAfter(maxDate, "day");
}

function isTimeDisabled(time, disabledTimes) {
  var l = disabledTimes.length;

  for (var i = 0; i < l; i++) {
    if (disabledTimes[i].get("hours") === time.get("hours") && disabledTimes[i].get("minutes") === time.get("minutes")) {
      return true;
    }
  }

  return false;
}

function isTimeInDisabledRange(time, _ref6) {
  var minTime = _ref6.minTime,
      maxTime = _ref6.maxTime;

  if (!minTime || !maxTime) {
    throw new Error("Both minTime and maxTime props required");
  }

  var base = (0, _moment.default)().hours(0).minutes(0).seconds(0);
  var baseTime = base.clone().hours(time.get("hours")).minutes(time.get("minutes"));
  var min = base.clone().hours(minTime.get("hours")).minutes(minTime.get("minutes"));
  var max = base.clone().hours(maxTime.get("hours")).minutes(maxTime.get("minutes"));
  return !(baseTime.isSameOrAfter(min) && baseTime.isSameOrBefore(max));
}

function allDaysDisabledBefore(day, unit) {
  var _ref7 = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {},
      minDate = _ref7.minDate,
      includeDates = _ref7.includeDates;

  var dateBefore = day.clone().subtract(1, unit);
  return minDate && dateBefore.isBefore(minDate, unit) || includeDates && includeDates.every(function (includeDate) {
    return dateBefore.isBefore(includeDate, unit);
  }) || false;
}

function allDaysDisabledAfter(day, unit) {
  var _ref8 = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {},
      maxDate = _ref8.maxDate,
      includeDates = _ref8.includeDates;

  var dateAfter = day.clone().add(1, unit);
  return maxDate && dateAfter.isAfter(maxDate, unit) || includeDates && includeDates.every(function (includeDate) {
    return dateAfter.isAfter(includeDate, unit);
  }) || false;
}

function getEffectiveMinDate(_ref9) {
  var minDate = _ref9.minDate,
      includeDates = _ref9.includeDates;

  if (includeDates && minDate) {
    return _moment.default.min(includeDates.filter(function (includeDate) {
      return minDate.isSameOrBefore(includeDate, "day");
    }));
  } else if (includeDates) {
    return _moment.default.min(includeDates);
  } else {
    return minDate;
  }
}

function getEffectiveMaxDate(_ref10) {
  var maxDate = _ref10.maxDate,
      includeDates = _ref10.includeDates;

  if (includeDates && maxDate) {
    return _moment.default.max(includeDates.filter(function (includeDate) {
      return maxDate.isSameOrAfter(includeDate, "day");
    }));
  } else if (includeDates) {
    return _moment.default.max(includeDates);
  } else {
    return maxDate;
  }
}

function getHightLightDaysMap() {
  var highlightDates = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : [];
  var defaultClassName = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : "react-datepicker__day--highlighted";
  var dateClasses = new Map();

  for (var i = 0, len = highlightDates.length; i < len; i++) {
    var obj = highlightDates[i];

    if (isMoment(obj)) {
      var key = obj.format("MM.DD.YYYY");
      var classNamesArr = dateClasses.get(key) || [];

      if (!classNamesArr.includes(defaultClassName)) {
        classNamesArr.push(defaultClassName);
        dateClasses.set(key, classNamesArr);
      }
    } else if ((0, _typeof2.default)(obj) === "object") {
      var keys = Object.keys(obj);
      var className = keys[0];
      var arrOfMoments = obj[keys[0]];

      if (typeof className === "string" && arrOfMoments.constructor === Array) {
        for (var k = 0, _len = arrOfMoments.length; k < _len; k++) {
          var _key = arrOfMoments[k].format("MM.DD.YYYY");

          var _classNamesArr = dateClasses.get(_key) || [];

          if (!_classNamesArr.includes(className)) {
            _classNamesArr.push(className);

            dateClasses.set(_key, _classNamesArr);
          }
        }
      }
    }
  }

  return dateClasses;
}

function timesToInjectAfter(startOfDay, currentTime, currentMultiplier, intervals, injectedTimes) {
  var l = injectedTimes.length;
  var times = [];

  for (var i = 0; i < l; i++) {
    var injectedTime = addMinutes(addHours(cloneDate(startOfDay), getHour(injectedTimes[i])), getMinute(injectedTimes[i]));
    var nextTime = addMinutes(cloneDate(startOfDay), (currentMultiplier + 1) * intervals);

    if (injectedTime.isBetween(currentTime, nextTime)) {
      times.push(injectedTimes[i]);
    }
  }

  return times;
}