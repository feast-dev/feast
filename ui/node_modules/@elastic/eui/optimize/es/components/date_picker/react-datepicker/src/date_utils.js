import _typeof from "@babel/runtime/helpers/typeof";

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
import moment from "moment";
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


export function newDate(point) {
  return moment(point);
}
export function newDateWithOffset(utcOffset) {
  return moment().utc().utcOffset(utcOffset);
}
export function now(maybeFixedUtcOffset) {
  if (maybeFixedUtcOffset == null) {
    return newDate();
  }

  return newDateWithOffset(maybeFixedUtcOffset);
}
export function cloneDate(date) {
  return date.clone();
}
export function parseDate(value, _ref) {
  var dateFormat = _ref.dateFormat,
      locale = _ref.locale,
      strictParsing = _ref.strictParsing;
  var m = moment(value, dateFormat, locale || moment.locale(), strictParsing);
  return m.isValid() ? m : null;
} // ** Date "Reflection" **

export function isMoment(date) {
  return moment.isMoment(date);
}
export function isDate(date) {
  return moment.isDate(date);
} // ** Date Formatting **

export function formatDate(date, format) {
  return date.format(format);
}
export function safeDateFormat(date, _ref2) {
  var dateFormat = _ref2.dateFormat,
      locale = _ref2.locale;
  return date && date.clone().locale(locale || moment.locale()).format(Array.isArray(dateFormat) ? dateFormat[0] : dateFormat) || "";
} // ** Date Setters **

export function setTime(date, _ref3) {
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
export function setMonth(date, month) {
  return set(date, "month", month);
}
export function setYear(date, year) {
  return set(date, "year", year);
}
export function setUTCOffset(date, offset) {
  return date.utcOffset(offset);
} // ** Date Getters **

export function getMillisecond(date) {
  return get(date, "millisecond");
}
export function getSecond(date) {
  return get(date, "second");
}
export function getMinute(date) {
  return get(date, "minute");
}
export function getHour(date) {
  return get(date, "hour");
} // Returns day of week

export function getDay(date) {
  return get(date, "day");
}
export function getWeek(date) {
  return get(date, "week");
}
export function getMonth(date) {
  return get(date, "month");
}
export function getYear(date) {
  return get(date, "year");
} // Returns day of month

export function getDate(date) {
  return get(date, "date");
}
export function getUTCOffset() {
  return moment().utcOffset();
}
export function getDayOfWeekCode(day) {
  return dayOfWeekCodes[day.isoWeekday()];
} // *** Start of ***

export function getStartOfDay(date) {
  return getStartOf(date, "day");
}
export function getStartOfWeek(date) {
  return getStartOf(date, "week");
}
export function getStartOfMonth(date) {
  return getStartOf(date, "month");
}
export function getStartOfDate(date) {
  return getStartOf(date, "date");
} // *** End of ***

export function getEndOfWeek(date) {
  return getEndOf(date, "week");
}
export function getEndOfMonth(date) {
  return getEndOf(date, "month");
} // ** Date Math **
// *** Addition ***

export function addMinutes(date, amount) {
  return add(date, amount, "minutes");
}
export function addHours(date, amount) {
  return add(date, amount, "hours");
}
export function addDays(date, amount) {
  return add(date, amount, "days");
}
export function addWeeks(date, amount) {
  return add(date, amount, "weeks");
}
export function addMonths(date, amount) {
  return add(date, amount, "months");
}
export function addYears(date, amount) {
  return add(date, amount, "years");
} // *** Subtraction ***

export function subtractDays(date, amount) {
  return subtract(date, amount, "days");
}
export function subtractWeeks(date, amount) {
  return subtract(date, amount, "weeks");
}
export function subtractMonths(date, amount) {
  return subtract(date, amount, "months");
}
export function subtractYears(date, amount) {
  return subtract(date, amount, "years");
} // ** Date Comparison **

export function isBefore(date1, date2) {
  return date1.isBefore(date2);
}
export function isAfter(date1, date2) {
  return date1.isAfter(date2);
}
export function equals(date1, date2) {
  return date1.isSame(date2);
}
export function isSameYear(date1, date2) {
  if (date1 && date2) {
    return date1.isSame(date2, "year");
  } else {
    return !date1 && !date2;
  }
}
export function isSameMonth(date1, date2) {
  if (date1 && date2) {
    return date1.isSame(date2, "month");
  } else {
    return !date1 && !date2;
  }
}
export function isSameDay(moment1, moment2) {
  if (moment1 && moment2) {
    return moment1.isSame(moment2, "day");
  } else {
    return !moment1 && !moment2;
  }
}
export function isSameTime(moment1, moment2) {
  if (moment1 && moment2) {
    return moment1.isSame(moment2, "second");
  } else {
    return !moment1 && !moment2;
  }
}
export function isSameUtcOffset(moment1, moment2) {
  if (moment1 && moment2) {
    return moment1.utcOffset() === moment2.utcOffset();
  } else {
    return !moment1 && !moment2;
  }
}
export function isDayInRange(day, startDate, endDate) {
  var before = startDate.clone().startOf("day").subtract(1, "seconds");
  var after = endDate.clone().startOf("day").add(1, "seconds");
  return day.clone().startOf("day").isBetween(before, after);
} // *** Diffing ***

export function getDaysDiff(date1, date2) {
  return getDiff(date1, date2, "days");
} // ** Date Localization **

export function localizeDate(date, locale) {
  return date.clone().locale(locale || moment.locale());
}
export function getDefaultLocale() {
  return moment.locale();
}
export function getDefaultLocaleData() {
  return moment.localeData();
}
export function registerLocale(localeName, localeData) {
  moment.defineLocale(localeName, localeData);
}
export function getLocaleData(date) {
  return date.localeData();
}
export function getLocaleDataForLocale(locale) {
  return moment.localeData(locale);
}
export function getFormattedWeekdayInLocale(locale, date, formatFunc) {
  return formatFunc(locale.weekdays(date));
}
export function getWeekdayMinInLocale(locale, date) {
  return locale.weekdaysMin(date);
}
export function getWeekdayShortInLocale(locale, date) {
  return locale.weekdaysShort(date);
} // TODO what is this format exactly?

export function getMonthInLocale(locale, date, format) {
  return locale.months(date, format);
}
export function getMonthShortInLocale(locale, date) {
  return locale.monthsShort(date);
} // ** Utils for some components **

export function isDayDisabled(day) {
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
export function isOutOfBounds(day) {
  var _ref5 = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {},
      minDate = _ref5.minDate,
      maxDate = _ref5.maxDate;

  return minDate && day.isBefore(minDate, "day") || maxDate && day.isAfter(maxDate, "day");
}
export function isTimeDisabled(time, disabledTimes) {
  var l = disabledTimes.length;

  for (var i = 0; i < l; i++) {
    if (disabledTimes[i].get("hours") === time.get("hours") && disabledTimes[i].get("minutes") === time.get("minutes")) {
      return true;
    }
  }

  return false;
}
export function isTimeInDisabledRange(time, _ref6) {
  var minTime = _ref6.minTime,
      maxTime = _ref6.maxTime;

  if (!minTime || !maxTime) {
    throw new Error("Both minTime and maxTime props required");
  }

  var base = moment().hours(0).minutes(0).seconds(0);
  var baseTime = base.clone().hours(time.get("hours")).minutes(time.get("minutes"));
  var min = base.clone().hours(minTime.get("hours")).minutes(minTime.get("minutes"));
  var max = base.clone().hours(maxTime.get("hours")).minutes(maxTime.get("minutes"));
  return !(baseTime.isSameOrAfter(min) && baseTime.isSameOrBefore(max));
}
export function allDaysDisabledBefore(day, unit) {
  var _ref7 = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {},
      minDate = _ref7.minDate,
      includeDates = _ref7.includeDates;

  var dateBefore = day.clone().subtract(1, unit);
  return minDate && dateBefore.isBefore(minDate, unit) || includeDates && includeDates.every(function (includeDate) {
    return dateBefore.isBefore(includeDate, unit);
  }) || false;
}
export function allDaysDisabledAfter(day, unit) {
  var _ref8 = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {},
      maxDate = _ref8.maxDate,
      includeDates = _ref8.includeDates;

  var dateAfter = day.clone().add(1, unit);
  return maxDate && dateAfter.isAfter(maxDate, unit) || includeDates && includeDates.every(function (includeDate) {
    return dateAfter.isAfter(includeDate, unit);
  }) || false;
}
export function getEffectiveMinDate(_ref9) {
  var minDate = _ref9.minDate,
      includeDates = _ref9.includeDates;

  if (includeDates && minDate) {
    return moment.min(includeDates.filter(function (includeDate) {
      return minDate.isSameOrBefore(includeDate, "day");
    }));
  } else if (includeDates) {
    return moment.min(includeDates);
  } else {
    return minDate;
  }
}
export function getEffectiveMaxDate(_ref10) {
  var maxDate = _ref10.maxDate,
      includeDates = _ref10.includeDates;

  if (includeDates && maxDate) {
    return moment.max(includeDates.filter(function (includeDate) {
      return maxDate.isSameOrAfter(includeDate, "day");
    }));
  } else if (includeDates) {
    return moment.max(includeDates);
  } else {
    return maxDate;
  }
}
export function getHightLightDaysMap() {
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
    } else if (_typeof(obj) === "object") {
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
export function timesToInjectAfter(startOfDay, currentTime, currentMultiplier, intervals, injectedTimes) {
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