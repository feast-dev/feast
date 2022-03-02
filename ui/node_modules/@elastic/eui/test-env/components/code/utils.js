"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.highlightByLine = exports.parseLineRanges = exports.nodeToHtml = exports.isAstElement = exports.getHtmlContent = exports.checkSupportedLanguage = exports.DEFAULT_LANGUAGE = exports.SUPPORTED_LANGUAGES = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = _interopRequireWildcard(require("react"));

var _refractor = require("refractor");

var _classnames = _interopRequireDefault(require("classnames"));

var _react2 = require("@emotion/react");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var SUPPORTED_LANGUAGES = (0, _refractor.listLanguages)();
exports.SUPPORTED_LANGUAGES = SUPPORTED_LANGUAGES;
var DEFAULT_LANGUAGE = 'text';
exports.DEFAULT_LANGUAGE = DEFAULT_LANGUAGE;

var checkSupportedLanguage = function checkSupportedLanguage(language) {
  return SUPPORTED_LANGUAGES.includes(language) ? language : DEFAULT_LANGUAGE;
};

exports.checkSupportedLanguage = checkSupportedLanguage;

var getHtmlContent = function getHtmlContent(data, children) {
  if (!Array.isArray(data) || data.length < 1) {
    return children;
  }

  return data.map(nodeToHtml);
};

exports.getHtmlContent = getHtmlContent;

var isAstElement = function isAstElement(node) {
  return node.hasOwnProperty('type') && node.type === 'element';
};

exports.isAstElement = isAstElement;

var nodeToHtml = function nodeToHtml(node, idx, nodes) {
  var depth = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 0;
  var key = "node-".concat(depth, "-").concat(idx);

  if (isAstElement(node)) {
    var properties = node.properties,
        tagName = node.tagName,
        children = node.children;
    return /*#__PURE__*/(0, _react.createElement)(tagName, _objectSpread(_objectSpread({}, properties), {}, {
      key: key,
      className: (0, _classnames.default)(properties.className)
    }), children && children.map(function (el, i) {
      return nodeToHtml(el, i, nodes, depth + 1);
    }));
  }

  return (0, _react2.jsx)(_react.default.Fragment, {
    key: key
  }, node.value);
};
/**
 * Line utils specific to EuiCodeBlock
 */


exports.nodeToHtml = nodeToHtml;
// Approximate width of a single digit/character
var CHAR_SIZE = 8;
var $euiSizeS = 8; // Creates an array of numbers from comma-separeated
// string of numbers or number ranges using `-`
// (e.g., "1, 3-10, 15")

var parseLineRanges = function parseLineRanges(ranges) {
  var highlights = [];
  ranges.replace(/\s/g, '').split(',').forEach(function (line) {
    if (line.includes('-')) {
      var range = line.split('-').map(Number);

      for (var i = range[0]; i <= range[1]; i++) {
        highlights.push(i);
      }
    } else {
      highlights.push(Number(line));
    }
  });
  return highlights;
};

exports.parseLineRanges = parseLineRanges;

var addLineData = function addLineData(nodes, data) {
  return nodes.reduce(function (result, node) {
    var lineStart = data.lineNumber;

    if (node.type === 'text') {
      if (!node.value.match(/\r\n?|\n/)) {
        node.lineStart = lineStart;
        node.lineEnd = lineStart;
        result.push(node);
      } else {
        var lines = node.value.split(/\r\n?|\n/);
        lines.forEach(function (line, i) {
          var num = i === 0 ? data.lineNumber : ++data.lineNumber;
          result.push({
            type: 'text',
            value: i === lines.length - 1 ? line : "".concat(line, "\n"),
            lineStart: num,
            lineEnd: num
          });
        });
      }

      return result;
    }

    if (node.children && node.children.length) {
      var _first$lineStart, _last$lineEnd;

      var children = addLineData(node.children, data);
      var first = children[0];
      var last = children[children.length - 1];
      var start = (_first$lineStart = first.lineStart) !== null && _first$lineStart !== void 0 ? _first$lineStart : lineStart;
      var end = (_last$lineEnd = last.lineEnd) !== null && _last$lineEnd !== void 0 ? _last$lineEnd : lineStart;

      if (start !== end) {
        children.forEach(function (node) {
          result.push(node);
        });
      } else {
        node.lineStart = start;
        node.lineEnd = end;
        node.children = children;
        result.push(node);
      }

      return result;
    }

    result.push(node);
    return result;
  }, []);
};

function wrapLines(nodes, options) {
  var highlights = options.highlight ? parseLineRanges(options.highlight) : [];
  var grouped = [];
  nodes.forEach(function (node) {
    var lineStart = node.lineStart - 1;

    if (grouped[lineStart]) {
      grouped[lineStart].push(node);
    } else {
      grouped[lineStart] = [node];
    }
  });
  var wrapped = [];
  var digits = grouped.length.toString().length;
  var width = digits * CHAR_SIZE;
  grouped.forEach(function (node, i) {
    var _properties;

    var lineNumber = i + 1;
    var classes = (0, _classnames.default)('euiCodeBlock__line', {
      'euiCodeBlock__line--isHighlighted': highlights.includes(lineNumber)
    });
    var children = options.showLineNumbers ? [{
      type: 'element',
      tagName: 'span',
      properties: (_properties = {
        style: {
          width: width
        }
      }, (0, _defineProperty2.default)(_properties, 'data-line-number', lineNumber), (0, _defineProperty2.default)(_properties, 'aria-hidden', true), (0, _defineProperty2.default)(_properties, "className", ['euiCodeBlock__lineNumber']), _properties),
      children: []
    }, {
      type: 'element',
      tagName: 'span',
      properties: {
        style: {
          marginLeft: width + $euiSizeS,
          width: "calc(100% - ".concat(width, "px)")
        },
        className: ['euiCodeBlock__lineText']
      },
      children: node
    }] : node;
    wrapped.push({
      type: 'element',
      tagName: 'span',
      properties: {
        className: [classes]
      },
      children: children
    });
  });
  return wrapped;
}

var highlightByLine = function highlightByLine(children, language, data) {
  return wrapLines(addLineData((0, _refractor.highlight)(children, language), {
    lineNumber: data.start
  }), {
    showLineNumbers: data.show,
    highlight: data.highlight
  });
};

exports.highlightByLine = highlightByLine;