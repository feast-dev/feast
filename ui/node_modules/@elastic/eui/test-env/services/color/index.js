"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  isColorDark: true,
  isValidHex: true,
  hexToHsv: true,
  hexToRgb: true,
  hsvToHex: true,
  hsvToRgb: true,
  rgbToHex: true,
  rgbToHsv: true,
  calculateContrast: true,
  calculateLuminance: true,
  VISUALIZATION_COLORS: true,
  DEFAULT_VISUALIZATION_COLOR: true,
  colorPalette: true,
  euiPaletteForLightBackground: true,
  euiPaletteForDarkBackground: true,
  euiPaletteColorBlind: true,
  euiPaletteColorBlindBehindText: true,
  euiPaletteForStatus: true,
  euiPaletteForTemperature: true,
  euiPaletteComplimentary: true,
  euiPaletteNegative: true,
  euiPalettePositive: true,
  euiPaletteCool: true,
  euiPaletteWarm: true,
  euiPaletteGray: true,
  getSteppedGradient: true
};
Object.defineProperty(exports, "isColorDark", {
  enumerable: true,
  get: function get() {
    return _is_color_dark.isColorDark;
  }
});
Object.defineProperty(exports, "isValidHex", {
  enumerable: true,
  get: function get() {
    return _is_valid_hex.isValidHex;
  }
});
Object.defineProperty(exports, "hexToHsv", {
  enumerable: true,
  get: function get() {
    return _hex_to_hsv.hexToHsv;
  }
});
Object.defineProperty(exports, "hexToRgb", {
  enumerable: true,
  get: function get() {
    return _hex_to_rgb.hexToRgb;
  }
});
Object.defineProperty(exports, "hsvToHex", {
  enumerable: true,
  get: function get() {
    return _hsv_to_hex.hsvToHex;
  }
});
Object.defineProperty(exports, "hsvToRgb", {
  enumerable: true,
  get: function get() {
    return _hsv_to_rgb.hsvToRgb;
  }
});
Object.defineProperty(exports, "rgbToHex", {
  enumerable: true,
  get: function get() {
    return _rgb_to_hex.rgbToHex;
  }
});
Object.defineProperty(exports, "rgbToHsv", {
  enumerable: true,
  get: function get() {
    return _rgb_to_hsv.rgbToHsv;
  }
});
Object.defineProperty(exports, "calculateContrast", {
  enumerable: true,
  get: function get() {
    return _luminance_and_contrast.calculateContrast;
  }
});
Object.defineProperty(exports, "calculateLuminance", {
  enumerable: true,
  get: function get() {
    return _luminance_and_contrast.calculateLuminance;
  }
});
Object.defineProperty(exports, "VISUALIZATION_COLORS", {
  enumerable: true,
  get: function get() {
    return _visualization_colors.VISUALIZATION_COLORS;
  }
});
Object.defineProperty(exports, "DEFAULT_VISUALIZATION_COLOR", {
  enumerable: true,
  get: function get() {
    return _visualization_colors.DEFAULT_VISUALIZATION_COLOR;
  }
});
Object.defineProperty(exports, "colorPalette", {
  enumerable: true,
  get: function get() {
    return _color_palette.colorPalette;
  }
});
Object.defineProperty(exports, "euiPaletteForLightBackground", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteForLightBackground;
  }
});
Object.defineProperty(exports, "euiPaletteForDarkBackground", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteForDarkBackground;
  }
});
Object.defineProperty(exports, "euiPaletteColorBlind", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteColorBlind;
  }
});
Object.defineProperty(exports, "euiPaletteColorBlindBehindText", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteColorBlindBehindText;
  }
});
Object.defineProperty(exports, "euiPaletteForStatus", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteForStatus;
  }
});
Object.defineProperty(exports, "euiPaletteForTemperature", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteForTemperature;
  }
});
Object.defineProperty(exports, "euiPaletteComplimentary", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteComplimentary;
  }
});
Object.defineProperty(exports, "euiPaletteNegative", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteNegative;
  }
});
Object.defineProperty(exports, "euiPalettePositive", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPalettePositive;
  }
});
Object.defineProperty(exports, "euiPaletteCool", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteCool;
  }
});
Object.defineProperty(exports, "euiPaletteWarm", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteWarm;
  }
});
Object.defineProperty(exports, "euiPaletteGray", {
  enumerable: true,
  get: function get() {
    return _eui_palettes.euiPaletteGray;
  }
});
Object.defineProperty(exports, "getSteppedGradient", {
  enumerable: true,
  get: function get() {
    return _stepped_gradient.getSteppedGradient;
  }
});

var _is_color_dark = require("./is_color_dark");

var _is_valid_hex = require("./is_valid_hex");

var _hex_to_hsv = require("./hex_to_hsv");

var _hex_to_rgb = require("./hex_to_rgb");

var _hsv_to_hex = require("./hsv_to_hex");

var _hsv_to_rgb = require("./hsv_to_rgb");

var _rgb_to_hex = require("./rgb_to_hex");

var _rgb_to_hsv = require("./rgb_to_hsv");

var _luminance_and_contrast = require("./luminance_and_contrast");

var _visualization_colors = require("./visualization_colors");

var _color_palette = require("./color_palette");

var _eui_palettes = require("./eui_palettes");

require("./color_types");

var _stepped_gradient = require("./stepped_gradient");

var _manipulation = require("./manipulation");

Object.keys(_manipulation).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _manipulation[key];
    }
  });
});

var _contrast = require("./contrast");

Object.keys(_contrast).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _contrast[key];
    }
  });
});