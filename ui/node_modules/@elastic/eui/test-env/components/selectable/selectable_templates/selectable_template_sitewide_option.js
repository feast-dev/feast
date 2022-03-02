"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.euiSelectableTemplateSitewideRenderOptions = exports.euiSelectableTemplateSitewideFormatOptions = void 0;

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = _interopRequireDefault(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _icon = require("../../../components/icon");

var _avatar = require("../../../components/avatar/avatar");

var _highlight = require("../../../components/highlight");

var _react2 = require("@emotion/react");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var euiSelectableTemplateSitewideFormatOptions = function euiSelectableTemplateSitewideFormatOptions(options) {
  return options.map(function (item) {
    var title = item.label;

    if (item.meta && item.meta.length) {
      title += " \u2022".concat(renderOptionMeta(item.meta, '', true));
    }

    return _objectSpread(_objectSpread({
      key: item.label,
      title: title
    }, item), {}, {
      className: (0, _classnames.default)('euiSelectableTemplateSitewide__listItem', item.className),
      prepend: item.icon ? (0, _react2.jsx)(_icon.EuiIcon, (0, _extends2.default)({
        color: "subdued",
        size: "l"
      }, item.icon)) : item.prepend,
      append: item.avatar ? (0, _react2.jsx)(_avatar.EuiAvatar, (0, _extends2.default)({
        type: "space",
        size: "s"
      }, item.avatar)) : item.append
    });
  });
};

exports.euiSelectableTemplateSitewideFormatOptions = euiSelectableTemplateSitewideFormatOptions;

var euiSelectableTemplateSitewideRenderOptions = function euiSelectableTemplateSitewideRenderOptions(option, searchValue) {
  return (0, _react2.jsx)(_react.default.Fragment, null, (0, _react2.jsx)(_highlight.EuiHighlight, {
    className: "euiSelectableTemplateSitewide__listItemTitle",
    search: searchValue
  }, option.label), renderOptionMeta(option.meta, searchValue));
};

exports.euiSelectableTemplateSitewideRenderOptions = euiSelectableTemplateSitewideRenderOptions;

function renderOptionMeta(meta) {
  var searchValue = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '';
  var stringsOnly = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
  if (!meta || meta.length < 1) return;
  var metas = meta.map(function (meta) {
    var text = meta.text,
        highlightSearchString = meta.highlightSearchString,
        className = meta.className,
        rest = (0, _objectWithoutProperties2.default)(meta, ["text", "highlightSearchString", "className"]);

    if (stringsOnly) {
      return " ".concat(text);
    } // Start with the base and custom classes


    var metaClasses = (0, _classnames.default)('euiSelectableTemplateSitewide__optionMeta', className); // If they provided a type, create the class and append

    if (meta.type) {
      metaClasses = (0, _classnames.default)(["euiSelectableTemplateSitewide__optionMeta--".concat(meta.type)], metaClasses);
    }

    return (0, _react2.jsx)(_highlight.EuiHighlight, (0, _extends2.default)({
      search: highlightSearchString ? searchValue : '',
      className: metaClasses,
      key: text
    }, rest), text);
  });
  return stringsOnly ? metas : (0, _react2.jsx)("span", {
    className: "euiSelectableTemplateSitewide__optionMetasList"
  }, metas);
}