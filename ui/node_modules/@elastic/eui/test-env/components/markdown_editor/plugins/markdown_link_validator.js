"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.markdownLinkValidator = markdownLinkValidator;
exports.mutateLinkToText = mutateLinkToText;
exports.validateUrl = validateUrl;

var _unistUtilVisit = _interopRequireDefault(require("unist-util-visit"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function markdownLinkValidator() {
  return function (ast) {
    (0, _unistUtilVisit.default)(ast, 'link', function (_node) {
      var node = _node;

      if (!validateUrl(node.url)) {
        mutateLinkToText(node);
      }
    });
  };
}

function mutateLinkToText(node) {
  var _;

  node.type = 'text';
  node.value = "[".concat(((_ = node.children[0]) === null || _ === void 0 ? void 0 : _.value) || '', "](").concat(node.url, ")");
  delete node.children;
  delete node.title;
  delete node.url;
  return node;
}

function validateUrl(url) {
  // A link is valid if it starts with http:, https:, or /
  return /^(https?:|\/)/.test(url);
}