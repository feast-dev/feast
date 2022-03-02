"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.processStringToChildren = processStringToChildren;

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _react = require("react");

var _predicate = require("../../services/predicate");

var _reactIs = require("react-is");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function isPrimitive(value) {
  return (0, _predicate.isBoolean)(value) || (0, _predicate.isString)(value) || (0, _predicate.isNumber)(value) || (0, _predicate.isUndefined)(value);
}

function hasPropName(child) {
  return child ? (0, _typeof2.default)(child) === 'object' && child.hasOwnProperty('propName') : false;
}
/**
 * Replaces placeholder values in `input` with their matching value in `values`
 * e.g. input:'Hello, {name}' will replace `{name}` with `values[name]`
 * @param {string} input
 * @param {RenderableValues} values
 * @param {Function} i18nMappingFunc
 * @returns {string | React.ReactChild[]}
 */


function processStringToChildren(input, values, i18nMappingFunc) {
  var children = [];
  var child;

  function appendCharToChild(char) {
    if (child === undefined) {
      // starting a new string literal
      child = char;
    } else if (typeof child === 'string') {
      // existing string literal
      child = child + char;
    } else if (hasPropName(child)) {
      // adding to the propName of a values lookup
      child.propName = child.propName + char;
    }
  }

  function appendValueToChildren(value) {
    if (value === undefined) {
      return;
    } else if ((0, _reactIs.isElement)(value)) {
      // an array with any ReactElements will be kept as an array
      // so they need to be assigned a key
      children.push( /*#__PURE__*/(0, _react.cloneElement)(value, {
        key: children.length
      }));
    } else if (hasPropName(value)) {// this won't be called, propName children are converted to a ReactChild before calling this
    } else {
      // everything else can go straight in
      if (i18nMappingFunc !== undefined && typeof value === 'string') {
        value = i18nMappingFunc(value);
      }

      children.push(value);
    }
  } // if we don't encounter a non-primitive
  // then `children` can be concatenated together at the end


  var encounteredNonPrimitive = false;

  for (var i = 0; i < input.length; i++) {
    var char = input[i];

    if (char === '\\') {
      // peek at the next character to know if this is an escape
      var nextChar = input[i + 1];
      var charToAdd = char; // if this isn't an escape sequence then we will add the backslash

      if (nextChar === '{' || nextChar === '}') {
        // escaping a brace
        i += 1; // advance passed the brace

        charToAdd = input[i];
      }

      appendCharToChild(charToAdd);
    } else if (char === '{') {
      appendValueToChildren(child);
      child = {
        propName: ''
      };
    } else if (char === '}') {
      var propName = child.propName;

      if (!values.hasOwnProperty(propName)) {
        throw new Error("Key \"".concat(propName, "\" not found in ").concat(JSON.stringify(values, null, 2)));
      }

      var propValue = values[propName];
      encounteredNonPrimitive = encounteredNonPrimitive || !isPrimitive(propValue);
      appendValueToChildren(propValue);
      child = undefined;
    } else {
      appendCharToChild(char);
    }
  } // include any remaining child value


  appendValueToChildren(child);
  return encounteredNonPrimitive ? children : children.join('');
}