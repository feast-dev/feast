function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { COLOR_MODES_STANDARD, COLOR_MODES_INVERSE } from './types';
export var DEFAULT_COLOR_MODE = COLOR_MODES_STANDARD.light;
/**
 * Returns whether the parameter is an object
 * @param {any} obj - Anything
 */

var isObject = function isObject(obj) {
  return obj && _typeof(obj) === 'object';
};
/**
 * Returns whether the provided color mode is `inverse`
 * @param {string} colorMode - `light`, `dark`, or `inverse`
 */


export var isInverseColorMode = function isInverseColorMode(colorMode) {
  return colorMode === COLOR_MODES_INVERSE;
};
/**
 * Returns the color mode configured in the current EuiThemeProvider.
 * Returns the parent color mode if none is explicity set.
 * @param {string} coloMode - `light`, `dark`, or `inverse`
 * @param {string} parentColorMode - `light`, `dark`, or `inverse`; used as the fallback
 */

export var getColorMode = function getColorMode(colorMode, parentColorMode) {
  var mode = colorMode === null || colorMode === void 0 ? void 0 : colorMode.toUpperCase();

  if (mode == null) {
    return parentColorMode || DEFAULT_COLOR_MODE;
  } else if (isInverseColorMode(mode)) {
    return parentColorMode === COLOR_MODES_STANDARD.dark || parentColorMode === undefined ? COLOR_MODES_STANDARD.light : COLOR_MODES_STANDARD.dark;
  } else {
    return mode;
  }
};
/**
 * Returns a value at a given path on an object.
 * If `colorMode` is provided, will scope the value to the appropriate color mode key (LIGHT\DARK)
 * @param {object} model - Object
 * @param {string} _path - Dot-notated string to a path on the object
 * @param {string} colorMode - `light` or `dark`
 */

export var getOn = function getOn(model, _path, colorMode) {
  var path = _path.split('.');

  var node = model;

  while (path.length) {
    var segment = path.shift();

    if (node.hasOwnProperty(segment) === false) {
      if (colorMode && node.hasOwnProperty(colorMode) === true && node[colorMode].hasOwnProperty(segment) === true) {
        if (node[colorMode][segment] instanceof Computed) {
          node = node[colorMode][segment].getValue(null, null, node, colorMode);
        } else {
          node = node[colorMode][segment];
        }
      } else {
        return undefined;
      }
    } else {
      if (node[segment] instanceof Computed) {
        node = node[segment].getValue(null, null, node, colorMode);
      } else {
        node = node[segment];
      }
    }
  }

  return node;
};
/**
 * Sets a value at a given path on an object.
 * @param {object} model - Object
 * @param {string} _path - Dot-notated string to a path on the object
 * @param {any} string -  The value to set
 */

export var setOn = function setOn(model, _path, value) {
  var path = _path.split('.');

  var propertyName = path.pop();
  var node = model;

  while (path.length) {
    var segment = path.shift();

    if (node.hasOwnProperty(segment) === false) {
      node[segment] = {};
    }

    node = node[segment];
  }

  node[propertyName] = value;
  return true;
};
/**
 * Creates a class to store the `computer` method and its eventual parameters.
 * Allows for on-demand computation with up-to-date parameters via `getValue` method.
 * @constructor
 * @param {function} computer - Function to be computed
 * @param {string | array} dependencies - Dependencies passed to the `computer` as parameters
 */

export var Computed = /*#__PURE__*/function () {
  function Computed(computer) {
    var dependencies = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];

    _classCallCheck(this, Computed);

    this.computer = computer;
    this.dependencies = dependencies;
  }
  /**
   * Executes the `computer` method with the current state of the theme
   * by taking into account previously computed values and modifications.
   * @param {Proxy | object} base - Computed or uncomputed theme
   * @param {Proxy | object} modifications - Theme value overrides
   * @param {object} working - Partially computed theme
   * @param {string} colorMode - `light` or `dark`
   */


  _createClass(Computed, [{
    key: "getValue",
    value: function getValue(base) {
      var modifications = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
      var working = arguments.length > 2 ? arguments[2] : undefined;
      var colorMode = arguments.length > 3 ? arguments[3] : undefined;

      if (!this.dependencies.length) {
        return this.computer(working);
      }

      if (!Array.isArray(this.dependencies)) {
        var _ref, _getOn;

        return this.computer((_ref = (_getOn = getOn(working, this.dependencies)) !== null && _getOn !== void 0 ? _getOn : getOn(modifications, this.dependencies, colorMode)) !== null && _ref !== void 0 ? _ref : getOn(base, this.dependencies, colorMode));
      }

      return this.computer(this.dependencies.map(function (dependency) {
        var _ref2, _getOn2;

        return (_ref2 = (_getOn2 = getOn(working, dependency)) !== null && _getOn2 !== void 0 ? _getOn2 : getOn(modifications, dependency, colorMode)) !== null && _ref2 !== void 0 ? _ref2 : getOn(base, dependency, colorMode);
      }));
    }
  }]);

  return Computed;
}();
/**
 * Returns a Class (`Computed`) that stores the arbitrary computer method
 * and references to its optional dependecies.
 * @param {function} computer - Arbitrary method to be called at compute time.
 * @param {string | array} dependencies - Values that will be provided to `computer` at compute time.
 */

export function computed(comp, dep) {
  return new Computed(comp, dep);
}
/**
 * Takes an uncomputed theme, and computes and returns all values taking
 * into consideration value overrides and configured color mode.
 * Overrides take precedence, and only values in the current color mode
 * are computed and returned.
 * @param {Proxy} base - Object to transform into Proxy
 * @param {Proxy | object} over - Unique identifier or name
 * @param {string} colorMode - `light` or `dark`
 */

export var getComputed = function getComputed(base, over, colorMode) {
  var output = {
    themeName: base.key
  };

  function loop(base, over) {
    var checkExisting = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
    var path = arguments.length > 3 ? arguments[3] : undefined;
    Object.keys(base).forEach(function (key) {
      var newPath = path ? "".concat(path, ".").concat(key) : "".concat(key);

      if ([].concat(_toConsumableArray(Object.values(COLOR_MODES_STANDARD)), [colorMode]).includes(key)) {
        if (key !== colorMode) {
          return;
        } else {
          var colorModeSegment = new RegExp("(\\.".concat(colorMode, "\\b)|(\\b").concat(colorMode, "\\.)"));
          newPath = newPath.replace(colorModeSegment, '');
        }
      }

      var existing = checkExisting && getOn(output, newPath);

      if (!existing || isObject(existing)) {
        var baseValue = base[key] instanceof Computed ? base[key].getValue(base.root, over.root, output, colorMode) : base[key];
        var overValue = over[key] instanceof Computed ? over[key].getValue(base.root, over.root, output, colorMode) : over[key];

        if (isObject(baseValue) && !Array.isArray(baseValue)) {
          loop(baseValue, overValue !== null && overValue !== void 0 ? overValue : {}, checkExisting, newPath);
        } else {
          setOn(output, newPath, overValue !== null && overValue !== void 0 ? overValue : baseValue);
        }
      }
    });
  } // Compute standard theme values and apply overrides


  loop(base, over); // Compute and apply extension values only

  loop(over, {}, true);
  return output;
};
/**
 * Builds a Proxy with a custom `handler` designed to self-reference values
 * and prevent arbitrary value overrides.
 * @param {object} model - Object to transform into Proxy
 * @param {string} key - Unique identifier or name
 */

export var buildTheme = function buildTheme(model, key) {
  var handler = {
    getPrototypeOf: function getPrototypeOf(target) {
      return Reflect.getPrototypeOf(target.model);
    },
    setPrototypeOf: function setPrototypeOf(target, prototype) {
      return Reflect.setPrototypeOf(target.model, prototype);
    },
    isExtensible: function isExtensible(target) {
      return Reflect.isExtensible(target);
    },
    preventExtensions: function preventExtensions(target) {
      return Reflect.preventExtensions(target.model);
    },
    getOwnPropertyDescriptor: function getOwnPropertyDescriptor(target, key) {
      return Reflect.getOwnPropertyDescriptor(target.model, key);
    },
    defineProperty: function defineProperty(target, property, attributes) {
      return Reflect.defineProperty(target.model, property, attributes);
    },
    has: function has(target, property) {
      return Reflect.has(target.model, property);
    },
    get: function get(_target, property) {
      if (property === 'key') {
        return _target[property];
      } // prevent Safari from locking up when the proxy is used in dev tools
      // as it doesn't support getPrototypeOf


      if (property === '__proto__') return {};
      var target = property === 'root' ? _target : _target.model || _target; // @ts-ignore `string` index signature

      var value = target[property];

      if (isObject(value) && !Array.isArray(value)) {
        return new Proxy({
          model: value,
          root: _target.root,
          key: "_".concat(_target.key)
        }, handler);
      } else {
        return value;
      }
    },
    set: function set(target) {
      return target;
    },
    deleteProperty: function deleteProperty(target) {
      return target;
    },
    ownKeys: function ownKeys(target) {
      return Reflect.ownKeys(target.model);
    },
    apply: function apply(target) {
      return target;
    },
    construct: function construct(target) {
      return target;
    }
  };
  var themeProxy = new Proxy({
    model: model,
    root: model,
    key: key
  }, handler);
  return themeProxy;
};
/**
 * Deeply merges two objects, using `source` values whenever possible.
 * @param {object} _target - Object with fallback values
 * @param {object} source - Object with desired values
 */

export var mergeDeep = function mergeDeep(_target) {
  var source = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

  var target = _objectSpread({}, _target);

  if (!isObject(target) || !isObject(source)) {
    return source;
  }

  Object.keys(source).forEach(function (key) {
    var targetValue = target[key];
    var sourceValue = source[key];

    if (isObject(targetValue) && isObject(sourceValue)) {
      target[key] = mergeDeep(_objectSpread({}, targetValue), _objectSpread({}, sourceValue));
    } else {
      target[key] = sourceValue;
    }
  });
  return target;
};