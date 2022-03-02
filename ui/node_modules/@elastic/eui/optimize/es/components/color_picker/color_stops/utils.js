import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { getEventPosition, getChromaColor } from '../utils';
import { DEFAULT_VISUALIZATION_COLOR } from '../../../services';
import { EUI_THUMB_SIZE } from '../../form/range/utils';
export var removeStop = function removeStop(colorStops, index) {
  if (colorStops.length === 1) {
    return colorStops;
  }

  return [].concat(_toConsumableArray(colorStops.slice(0, index)), _toConsumableArray(colorStops.slice(index + 1)));
};
export var addDefinedStop = function addDefinedStop(colorStops, stop) {
  var color = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : DEFAULT_VISUALIZATION_COLOR;
  var newStop = {
    stop: stop,
    color: color
  };
  colorStops = [].concat(_toConsumableArray(colorStops), [newStop]);
  colorStops.sort(function (a, b) {
    if (a.stop < b.stop) {
      return -1;
    }

    if (a.stop > b.stop) {
      return 1;
    }

    return 0;
  });
  return colorStops;
};
export var addStop = function addStop(colorStops) {
  var color = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : DEFAULT_VISUALIZATION_COLOR;
  var max = arguments.length > 2 ? arguments[2] : undefined;
  var index = colorStops.length ? colorStops.length - 1 : 0;
  var stops = colorStops.map(function (el) {
    return el.stop;
  });
  var currentStop = stops[index] != null ? stops[index] : max;
  var delta = 1;

  if (index !== 0) {
    var prevStop = stops[index - 1];
    delta = currentStop - prevStop;
  }

  var stop = currentStop + delta;

  if (stop > max) {
    stop = max;
  } // We've reached the max, so start working backwards


  while (stops.indexOf(stop) > -1) {
    stop--;
  }

  var newStop = {
    stop: stop,
    color: color
  };
  return [].concat(_toConsumableArray(colorStops.slice(0, index + 1)), [newStop], _toConsumableArray(colorStops.slice(index + 1)));
};
export var isColorInvalid = function isColorInvalid(color) {
  var showAlpha = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  return getChromaColor(color, showAlpha) == null || color === '';
};
export var isStopInvalid = function isStopInvalid(stop) {
  return stop == null || isNaN(stop);
};
export var isInvalid = function isInvalid(colorStops) {
  var showAlpha = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  return colorStops.some(function (colorStop) {
    return isColorInvalid(colorStop.color, showAlpha) || isStopInvalid(colorStop.stop);
  });
};
export var calculateScale = function calculateScale(trackWidth) {
  var thumbToTrackRatio = EUI_THUMB_SIZE / trackWidth;
  return (1 - thumbToTrackRatio) * 100;
};
export var getStopFromMouseLocation = function getStopFromMouseLocation(location, ref, min, max) {
  var box = getEventPosition(location, ref);
  return Math.round(box.left / box.width * (max - min) + min);
};
export var getPositionFromStop = function getPositionFromStop(stop, ref, min, max) {
  // For wide implementations, integer percentages can be visually off.
  // Use 1 decimal place for more accuracy
  return parseFloat(((stop - min) / (max - min) * calculateScale(ref && ref.clientWidth > 0 ? ref.clientWidth : 100)).toFixed(1));
};