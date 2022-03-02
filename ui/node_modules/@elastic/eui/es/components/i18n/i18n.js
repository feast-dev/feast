function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Fragment, useContext } from 'react';
import { EuiI18nConsumer } from '../context';
import { I18nContext } from '../context/context';
import { processStringToChildren } from './i18n_util';
import { jsx as ___EmotionJSX } from "@emotion/react";

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

  var children = processStringToChildren(renderable, values, i18nMappingFunc);

  if (typeof children === 'string') {
    // likewise, `processStringToChildren` returns a string or ReactChild[] depending on
    // the type of `values`, so we will make the assumption that the default value is correct.
    return children;
  }

  var Component = render ? render(children) : function () {
    return ___EmotionJSX(Fragment, null, children);
  }; // same reasons as above, we can't promise the transforms match the default's type

  return /*#__PURE__*/React.createElement(Component, values);
}

function isI18nTokensShape(x) {
  return x.tokens != null;
} // Must use the generics <T extends {}>
// If instead typed with React.FunctionComponent there isn't feedback given back to the dev
// when using a `values` object with a renderer callback.


var EuiI18n = function EuiI18n(props) {
  return ___EmotionJSX(EuiI18nConsumer, null, function (i18nConfig) {
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


function useEuiI18n() {
  var i18nConfig = useContext(I18nContext);
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
        _ref2 = _slicedToArray(_ref, 2),
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

export { EuiI18n, useEuiI18n };