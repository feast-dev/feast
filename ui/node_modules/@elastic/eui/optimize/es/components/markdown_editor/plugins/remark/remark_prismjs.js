import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import refractor from 'refractor';
import visit from 'unist-util-visit';
export var FENCED_CLASS = 'remark-prismjs--fenced';

var attacher = function attacher() {
  return function (ast) {
    return visit(ast, 'code', visitor);
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
    data.hChildren = refractor.highlight(node.value, language);
    data.hProperties = _objectSpread(_objectSpread({}, data.hProperties), {}, {
      language: language,
      className: ['prismjs'].concat(_toConsumableArray(((_data$hProperties = data.hProperties) === null || _data$hProperties === void 0 ? void 0 : _data$hProperties.className) || []), ["language-".concat(language), FENCED_CLASS])
    });
  }
};

export default attacher;