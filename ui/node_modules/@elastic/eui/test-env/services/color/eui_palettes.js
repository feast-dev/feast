"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.euiPaletteGray = exports.euiPaletteWarm = exports.euiPaletteCool = exports.euiPalettePositive = exports.euiPaletteNegative = exports.euiPaletteComplimentary = exports.euiPaletteForTemperature = exports.euiPaletteForStatus = exports.euiPaletteForDarkBackground = exports.euiPaletteForLightBackground = exports.euiPaletteColorBlindBehindText = exports.euiPaletteColorBlind = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _chromaJs = _interopRequireDefault(require("chroma-js"));

var _color_palette = require("./color_palette");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var flatten = function flatten(arr) {
  var _ref;

  return (_ref = []).concat.apply(_ref, (0, _toConsumableArray2.default)(arr));
};

var euiPalette = function euiPalette(colors, steps) {
  var diverge = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
  var categorical = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : true;

  // This function also trims the first color so white/black is never a color
  if (!diverge && steps > 1) {
    var palette = (0, _color_palette.colorPalette)(colors, steps + 1);
    palette.shift();
    return palette;
  }

  return (0, _color_palette.colorPalette)(colors, steps, diverge, categorical);
};

var euiPaletteColorBlind = function euiPaletteColorBlind() {
  var _ref2 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      _ref2$rotations = _ref2.rotations,
      rotations = _ref2$rotations === void 0 ? 1 : _ref2$rotations,
      _ref2$order = _ref2.order,
      order = _ref2$order === void 0 ? 'append' : _ref2$order,
      _ref2$direction = _ref2.direction,
      direction = _ref2$direction === void 0 ? 'lighter' : _ref2$direction,
      _ref2$sortBy = _ref2.sortBy,
      sortBy = _ref2$sortBy === void 0 ? 'default' : _ref2$sortBy,
      _ref2$sortShift = _ref2.sortShift,
      sortShift = _ref2$sortShift === void 0 ? '-100' : _ref2$sortShift;

  var colors = [];
  var base = ['#54B399', // 0 green
  '#6092C0', // 1 blue
  '#D36086', // 2 dark pink
  '#9170B8', // 3 purple
  '#CA8EAE', // 4 light pink
  '#D6BF57', // 5 yellow
  '#B9A888', // 6 tan
  '#DA8B45', // 7 orange
  '#AA6556', // 8 brown
  '#E7664C' // 9 red
  ];

  if (sortBy === 'natural') {
    // Sort the colors based on the color wheel, but shifting the values based on sortShift
    base = (0, _toConsumableArray2.default)(base).sort(function (a, b) {
      return (0, _chromaJs.default)(a).set('hsl.h', sortShift).hsl()[0] - (0, _chromaJs.default)(b).set('hsl.h', sortShift).hsl()[0];
    });
  }

  if (rotations > 1) {
    var palettes = base.map(function (color) {
      // Create the darkest and lightest versions of each color using black and white
      var palette = (0, _color_palette.colorPalette)(['black', color, 'white'], 5, false, true); // Then removing the extremes

      palette.pop();
      palette.shift();

      switch (direction) {
        case 'lighter':
          return (0, _color_palette.colorPalette)([palette[1], palette[2]], rotations, false, true);

        case 'darker':
          return (0, _color_palette.colorPalette)([palette[1], palette[0]], rotations, false, true);

        case 'both':
          return (0, _color_palette.colorPalette)(palette, rotations, false, true);
      }
    });

    if (order === 'group') {
      colors = flatten(palettes);
    } else {
      var _loop = function _loop(i) {
        var _colors;

        var rotation = palettes.map(function (palette) {
          return palette[i];
        });

        (_colors = colors).push.apply(_colors, (0, _toConsumableArray2.default)(rotation));
      };

      for (var i = 0; i < rotations; i++) {
        _loop(i);
      }
    }
  } else {
    colors = base;
  }

  return colors;
};
/**
 * Color blind palette with text is meant for use when text is applied on top of the color.
 * It increases the brightness of the color to give the text more contrast.
 */


exports.euiPaletteColorBlind = euiPaletteColorBlind;

var euiPaletteColorBlindBehindText = function euiPaletteColorBlindBehindText() {
  var paletteProps = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
  var originalPalette = euiPaletteColorBlind(paletteProps);
  var newPalette = originalPalette.map(function (color) {
    return (0, _chromaJs.default)(color).brighten(0.5).hex();
  });
  return newPalette;
};

exports.euiPaletteColorBlindBehindText = euiPaletteColorBlindBehindText;

var euiPaletteForLightBackground = function euiPaletteForLightBackground() {
  return ['#006BB4', '#017D73', '#F5A700', '#BD271E', '#DD0A73'];
};

exports.euiPaletteForLightBackground = euiPaletteForLightBackground;

var euiPaletteForDarkBackground = function euiPaletteForDarkBackground() {
  return ['#1BA9F5', '#7DE2D1', '#F990C0', '#F66', '#FFCE7A'];
};

exports.euiPaletteForDarkBackground = euiPaletteForDarkBackground;
var positiveColor = '#209280';
var negativeColor = '#CC5642';
var lightNegativeColor = euiPaletteColorBlind()[9];
var coolArray = [euiPaletteColorBlind()[1], '#6092C0'];
var warmArray = [euiPaletteColorBlind()[7], euiPaletteColorBlind()[9]];

var euiPaletteForStatus = function euiPaletteForStatus(steps) {
  if (steps === 1) {
    return [positiveColor];
  }

  if (steps <= 3) {
    return euiPalette([positiveColor, euiPaletteColorBlind()[5], negativeColor], steps, true);
  }

  return euiPalette([positiveColor, euiPaletteColorBlind()[0], euiPaletteColorBlind()[5], lightNegativeColor, negativeColor], steps, true);
};

exports.euiPaletteForStatus = euiPaletteForStatus;

var euiPaletteForTemperature = function euiPaletteForTemperature(steps) {
  var cools = (0, _color_palette.colorPalette)([].concat((0, _toConsumableArray2.default)(coolArray.slice().reverse()), ['#EBEFF5']), 3);
  var warms = (0, _color_palette.colorPalette)(['#F4F3DB'].concat(warmArray), 3);

  if (steps === 1) {
    return [cools[0]];
  } else if (steps <= 3) {
    return euiPalette([cools[0], lightNegativeColor], steps, true);
  }

  return euiPalette([].concat((0, _toConsumableArray2.default)(cools), (0, _toConsumableArray2.default)(warms)), steps, true);
};

exports.euiPaletteForTemperature = euiPaletteForTemperature;

var euiPaletteComplimentary = function euiPaletteComplimentary(steps) {
  if (steps === 1) {
    return [euiPaletteColorBlind()[1]];
  }

  return euiPalette([euiPaletteColorBlind()[1], euiPaletteColorBlind()[7]], steps, true);
};

exports.euiPaletteComplimentary = euiPaletteComplimentary;

var euiPaletteNegative = function euiPaletteNegative(steps) {
  if (steps === 1) {
    return [lightNegativeColor];
  }

  return euiPalette(['white', negativeColor], steps);
};

exports.euiPaletteNegative = euiPaletteNegative;

var euiPalettePositive = function euiPalettePositive(steps) {
  if (steps === 1) {
    return [euiPaletteColorBlind()[0]];
  }

  return euiPalette(['white', positiveColor], steps);
};

exports.euiPalettePositive = euiPalettePositive;

var euiPaletteCool = function euiPaletteCool(steps) {
  if (steps === 1) {
    return [coolArray[1]];
  }

  return euiPalette(['white'].concat(coolArray), steps);
};

exports.euiPaletteCool = euiPaletteCool;

var euiPaletteWarm = function euiPaletteWarm(steps) {
  if (steps === 1) {
    return [lightNegativeColor];
  }

  return euiPalette(['#FBFBDC'].concat(warmArray), steps);
};

exports.euiPaletteWarm = euiPaletteWarm;

var euiPaletteGray = function euiPaletteGray(steps) {
  if (steps === 1) {
    return ['#98a2b3'];
  }

  return euiPalette(['white', '#d3dae6', '#98a2b3', '#69707d', '#343741'], steps, false);
};

exports.euiPaletteGray = euiPaletteGray;