"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useEuiI18n = useEuiI18n;
exports.EuiI18n = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _react = _interopRequireWildcard(require("react"));

var _context = require("../context");

var _context2 = require("../context/context");

var _i18n_util = require("./i18n_util");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function errorOnMissingValues(token) {
  throw new Error("I18n mapping for token \"".concat(token, "\" is a formatting function but no values were provided."));
}

function lookupToken(options) {
  var token = options.token,
      i18nMapping = options.i18nMapping,
      valueDefault = options.valueDefault,
      i18nMappingFunc = options.i18nMappingFunc,
      values = options.values,
      render = options.render;
  var renderable = i18nMapping && i18nMapping[token] || valueDefault;

  if (typeof renderable === 'function') {
    if (values === undefined) {
      return errorOnMissingValues(token);
    } // @ts-ignore TypeScript complains that `DEFAULT` doesn't have a call signature but we verified `renderable` is a function


    return renderable(values);
  } else if (values === undefined || typeof renderable !== 'string') {
    if (i18nMappingFunc && typeof valueDefault === 'string') {
      renderable = i18nMappingFunc(valueDefault);
    } // there's a hole in the typings here as there is no guarantee that i18nMappingFunc
    // returned the same type of the default value, but we need to keep that assumption


    return renderable;
  }

  var children = (0, _i18n_util.processStringToChildren)(renderable, values, i18nMappingFunc);

  if (typeof children === 'string') {
    // likewise, `processStringToChildren` returns a string or ReactChild[] depending on
    // the type of `values`, so we will make the assumption that the default value is correct.
    return children;
  }

  var Component = render ? render(children) : function () {
    return (0, _react2.jsx)(_react.Fragment, null, children);
  }; // same reasons as above, we can't promise the transforms match the default's type

  return /*#__PURE__*/_react.default.createElement(Component, values);
}

function isI18nTokensShape(x) {
  return x.tokens != null;
} // Must use the generics <T extends {}>
// If instead typed with React.FunctionComponent there isn't feedback given back to the dev
// when using a `values` object with a renderer callback.


var EuiI18n = function EuiI18n(props) {
  return (0, _react2.jsx)(_context.EuiI18nConsumer, null, function (i18nConfig) {
    var mapping = i18nConfig.mapping,
        mappingFunc = i18nConfig.mappingFunc,
        render = i18nConfig.render;

    if (isI18nTokensShape(props)) {
      return props.children(props.tokens.map(function (token, idx) {
        return lookupToken({
          token: token,
          i18nMapping: mapping,
          valueDefault: props.defaults[idx],
          render: render
        });
      }));
    }

    var tokenValue = lookupToken({
      token: props.token,
      i18nMapping: mapping,
      valueDefault: props.default,
      i18nMappingFunc: mappingFunc,
      values: props.values,
      render: render
    });

    if (props.children) {
      return props.children(tokenValue);
    } else {
      return tokenValue;
    }
  });
}; // A single default could be a string, react child, or render function


exports.EuiI18n = EuiI18n;

function useEuiI18n() {
  var i18nConfig = (0, _react.useContext)(_context2.I18nContext);
  var mapping = i18nConfig.mapping,
      mappingFunc = i18nConfig.mappingFunc,
      render = i18nConfig.render;

  for (var _len = arguments.length, props = new Array(_len), _key = 0; _key < _len; _key++) {
    props[_key] = arguments[_key];
  }

  if (typeof props[0] === 'string') {
    var _token = props[0],
        _defaultValue = props[1],
        _values = props[2];
    return lookupToken({
      token: _token,
      i18nMapping: mapping,
      valueDefault: _defaultValue,
      i18nMappingFunc: mappingFunc,
      values: _values,
      render: render
    });
  } else {
    var _ref = props,
        _ref2 = (0, _slicedToArray2.default)(_ref, 2),
        _tokens = _ref2[0],
        _defaultValues = _ref2[1];

    return _tokens.map(function (token, idx) {
      return lookupToken({
        token: token,
        i18nMapping: mapping,
        valueDefault: _defaultValues[idx],
        i18nMappingFunc: mappingFunc,
        render: render
      });
    });
  }
}