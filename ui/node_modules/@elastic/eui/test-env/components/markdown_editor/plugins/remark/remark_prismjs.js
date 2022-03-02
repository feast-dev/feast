"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.FENCED_CLASS = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _refractor = _interopRequireDefault(require("refractor"));

var _unistUtilVisit = _interopRequireDefault(require("unist-util-visit"));

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var FENCED_CLASS = 'remark-prismjs--fenced';
exports.FENCED_CLASS = FENCED_CLASS;

var attacher = function attacher() {
  return function (ast) {
    return (0, _unistUtilVisit.default)(ast, 'code', visitor);
  };

  function visitor(node) {
    var _data$hProperties;

    var _node$data = node.data,
        data = _node$data === void 0 ? {} : _node$data,
        language = node.lang;

    if (!language) {
      return;
    }

    node.data = data;
    data.hChildren = _refractor.default.highlight(node.value, language);
    data.hProperties = _objectSpread(_objectSpread({}, data.hProperties), {}, {
      language: language,
      className: ['prismjs'].concat((0, _toConsumableArray2.default)(((_data$hProperties = data.hProperties) === null || _data$hProperties === void 0 ? void 0 : _data$hProperties.className) || []), ["language-".concat(language), FENCED_CLASS])
    });
  }
};

var _default = attacher;
exports.default = _default;