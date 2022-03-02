/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { dateFormatAliases } from '../../../services/format'; // ESLint doesn't realise that we can import Moment directly.
// eslint-disable-next-line import/named

import moment from 'moment';
var utc = moment.utc;
var GRANULARITY_KEY = '__eui_granularity';
var FORMAT_KEY = '__eui_format';
export var Granularity = Object.freeze({
  DAY: {
    es: 'd',
    js: 'day',
    isSame: function isSame(d1, d2) {
      return d1.isSame(d2, 'day');
    },
    start: function start(date) {
      return date.startOf('day');
    },
    startOfNext: function startOfNext(date) {
      return date.add(1, 'days').startOf('day');
    },
    iso8601: function iso8601(date) {
      return date.format('YYYY-MM-DD');
    }
  },
  WEEK: {
    es: 'w',
    js: 'week',
    isSame: function isSame(d1, d2) {
      return d1.isSame(d2, 'week');
    },
    start: function start(date) {
      return date.startOf('week');
    },
    startOfNext: function startOfNext(date) {
      return date.add(1, 'weeks').startOf('week');
    },
    iso8601: function iso8601(date) {
      return date.format('YYYY-MM-DD');
    }
  },
  MONTH: {
    es: 'M',
    js: 'month',
    isSame: function isSame(d1, d2) {
      return d1.isSame(d2, 'month');
    },
    start: function start(date) {
      return date.startOf('month');
    },
    startOfNext: function startOfNext(date) {
      return date.add(1, 'months').startOf('month');
    },
    iso8601: function iso8601(date) {
      return date.format('YYYY-MM');
    }
  },
  YEAR: {
    es: 'y',
    js: 'year',
    isSame: function isSame(d1, d2) {
      return d1.isSame(d2, 'year');
    },
    start: function start(date) {
      return date.startOf('year');
    },
    startOfNext: function startOfNext(date) {
      return date.add(1, 'years').startOf('year');
    },
    iso8601: function iso8601(date) {
      return date.format('YYYY');
    }
  }
});

var parseTime = function parseTime(value) {
  var parsed = utc(value, ['HH:mm', 'H:mm', 'H:mm', 'h:mm a', 'h:mm A', 'hh:mm a', 'hh:mm A'], true);

  if (parsed.isValid()) {
    parsed[FORMAT_KEY] = parsed.creationData().format;
    return parsed;
  }
};

var parseDay = function parseDay(value) {
  var parsed;

  switch (value.toLowerCase()) {
    case 'today':
      parsed = utc().startOf('day');
      parsed[GRANULARITY_KEY] = Granularity.DAY;
      parsed[FORMAT_KEY] = value;
      return parsed;

    case 'yesterday':
      parsed = utc().subtract(1, 'days').startOf('day');
      parsed[GRANULARITY_KEY] = Granularity.DAY;
      parsed[FORMAT_KEY] = value;
      return parsed;

    case 'tomorrow':
      parsed = utc().add(1, 'days').startOf('day');
      parsed[GRANULARITY_KEY] = Granularity.DAY;
      parsed[FORMAT_KEY] = value;
      return parsed;

    default:
      parsed = utc(value, ['ddd', 'dddd', 'D MMM YY', 'Do MMM YY', 'D MMM YYYY', 'Do MMM YYYY', 'DD MMM YY', 'DD MMM YYYY', 'D MMMM YY', 'Do MMMM YY', 'D MMMM YYYY', 'Do MMMM YYYY', 'DD MMMM YY', 'DD MMMM YYYY', 'YYYY-MM-DD'], true);

      if (parsed.isValid()) {
        try {
          parsed[GRANULARITY_KEY] = Granularity.DAY;
          parsed[FORMAT_KEY] = parsed.creationData().format;
          return parsed;
        } catch (e) {
          console.error(e);
        }
      }

  }
};

var parseWeek = function parseWeek(value) {
  var parsed;

  switch (value.toLowerCase()) {
    case 'this week':
      parsed = utc();
      break;

    case 'last week':
      parsed = utc().subtract(1, 'weeks');
      break;

    case 'next week':
      parsed = utc().add(1, 'weeks');
      break;

    default:
      var match = value.match(/week (\d+)/i);

      if (match) {
        var weekNr = Number(match[1]);
        parsed = utc().weeks(weekNr);
      } else {
        return;
      }

  }

  if (parsed != null && parsed.isValid()) {
    parsed = parsed.startOf('week');
    parsed[GRANULARITY_KEY] = Granularity.WEEK;
    parsed[FORMAT_KEY] = parsed.creationData().format;
    return parsed;
  }
};

var parseMonth = function parseMonth(value) {
  var parsed;

  switch (value.toLowerCase()) {
    case 'this month':
      parsed = utc();
      break;

    case 'next month':
      parsed = utc().endOf('month').add(2, 'days');
      break;

    case 'last month':
      parsed = utc().startOf('month').subtract(2, 'days');
      break;

    default:
      parsed = utc(value, ['MMM', 'MMMM'], true);

      if (parsed.isValid()) {
        var now = utc();
        parsed.year(now.year());
      } else {
        parsed = utc(value, ['MMM YY', 'MMMM YY', 'MMM YYYY', 'MMMM YYYY', 'YYYY MMM', 'YYYY MMMM', 'YYYY-MM'], true);
      }

  }

  if (parsed.isValid()) {
    parsed.startOf('month');
    parsed[GRANULARITY_KEY] = Granularity.MONTH;
    parsed[FORMAT_KEY] = parsed.creationData().format;
    return parsed;
  }
};

var parseYear = function parseYear(value) {
  var parsed;

  switch (value.toLowerCase()) {
    case 'this year':
      parsed = utc().startOf('year');
      parsed[GRANULARITY_KEY] = Granularity.YEAR;
      parsed[FORMAT_KEY] = value;
      return parsed;

    case 'next year':
      parsed = utc().endOf('year').add(2, 'months').startOf('year');
      parsed[GRANULARITY_KEY] = Granularity.YEAR;
      parsed[FORMAT_KEY] = value;
      return parsed;

    case 'last year':
      parsed = utc().startOf('year').subtract(2, 'months').startOf('year');
      parsed[GRANULARITY_KEY] = Granularity.YEAR;
      parsed[FORMAT_KEY] = value;
      return parsed;

    default:
      parsed = utc(value, ['YY', 'YYYY'], true);

      if (parsed.isValid()) {
        parsed[GRANULARITY_KEY] = Granularity.YEAR;
        parsed[FORMAT_KEY] = parsed.creationData().format;
        return parsed;
      }

  }
};

var parseDefault = function parseDefault(value) {
  var parsed = utc(value, [moment.ISO_8601, moment.RFC_2822, 'DD MMM YY HH:mm', 'DD MMM YY HH:mm:ss', 'DD MMM YYYY HH:mm', 'DD MMM YYYY HH:mm:ss', 'DD MMMM YYYY HH:mm', 'DD MMMM YYYY HH:mm:ss'], true);

  if (!parsed.isValid()) {
    var time = Date.parse(value);
    var offset = moment(time).utcOffset();
    parsed = utc(time);
    parsed.add(offset, 'minutes');
  }

  if (parsed.isValid()) {
    parsed[FORMAT_KEY] = parsed.creationData().format;
  }

  return parsed;
};

var printDay = function printDay(now, date, format) {
  if (format.match(/yesterday|tomorrow|today/i)) {
    if (now.isSame(date, 'day')) {
      return 'today';
    }

    if (now.subtract(1, 'day').isSame(date, 'day')) {
      return 'yesterday';
    }

    if (now.add(1, 'day').isSame(date, 'day')) {
      return 'tomorrow';
    }

    if (now.isSame(date, 'week')) {
      return date.format('dddd');
    }
  }

  return date.format(format);
};

var printWeek = function printWeek(now, date, format) {
  if (format.match(/(?:this|next|last) week/i)) {
    if (now.isSame(date, 'week')) {
      return 'This Week';
    }

    if (now.startOf('week').subtract(2, 'days').isSame(date, 'week')) {
      return 'Last Week';
    }

    if (now.endOf('week').add(2, 'days').isSame(date, 'week')) {
      return 'Next Week';
    }
  }

  return date.format(format);
};

var printMonth = function printMonth(now, date, format) {
  if (format.match(/(?:this|next|last) month/i)) {
    if (now.isSame(date, 'month')) {
      return 'This Month';
    }

    if (now.startOf('month').subtract(2, 'days').isSame(date, 'month')) {
      return 'Last Month';
    }

    if (now.endOf('month').add(2, 'days').isSame(date, 'month')) {
      return 'Next Month';
    }
  }

  return date.format(format);
};

var printYear = function printYear(now, date, format) {
  if (format.match(/(?:this|next|last) year/i)) {
    if (now.isSame(date, 'year')) {
      return 'This Year';
    }

    if (now.startOf('year').subtract(2, 'months').isSame(date, 'year')) {
      return 'Last Year';
    }

    if (now.endOf('year').add(2, 'months').isSame(date, 'year')) {
      return 'Next Year';
    }
  }

  return date.format(format);
};

export var printIso8601 = function printIso8601(value) {
  return utc(value).format(moment.defaultFormatUtc);
};
export var dateGranularity = function dateGranularity(parsedDate) {
  return parsedDate[GRANULARITY_KEY];
};
export var dateFormat = Object.freeze({
  parse: function parse(value) {
    var parsed = parseDay(value) || parseMonth(value) || parseYear(value) || parseWeek(value) || parseTime(value) || parseDefault(value);

    if (!parsed) {
      throw new Error("could not parse [".concat(value, "] as date"));
    }

    return parsed;
  },
  print: function print(date) {
    var defaultGranularity = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : undefined;
    date = moment.isMoment(date) ? date : utc(date);
    var euiDate = date;
    var now = utc();
    var format = euiDate[FORMAT_KEY];

    if (!format) {
      return date.format(dateFormatAliases.iso8601);
    }

    var granularity = euiDate[GRANULARITY_KEY] || defaultGranularity;

    switch (granularity) {
      case Granularity.DAY:
        return printDay(now, date, format);

      case Granularity.WEEK:
        return printWeek(now, date, format);

      case Granularity.MONTH:
        return printMonth(now, date, format);

      case Granularity.YEAR:
        return printYear(now, date, format);

      default:
        return date.format(format);
    }
  }
});