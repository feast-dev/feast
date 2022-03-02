/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import visit from 'unist-util-visit';
export function markdownLinkValidator() {
  return function (ast) {
    visit(ast, 'link', function (_node) {
      var node = _node;

      if (!validateUrl(node.url)) {
        mutateLinkToText(node);
      }
    });
  };
}
export function mutateLinkToText(node) {
  var _;

  node.type = 'text';
  node.value = "[".concat(((_ = node.children[0]) === null || _ === void 0 ? void 0 : _.value) || '', "](").concat(node.url, ")");
  delete node.children;
  delete node.title;
  delete node.url;
  return node;
}
export function validateUrl(url) {
  // A link is valid if it starts with http:, https:, or /
  return /^(https?:|\/)/.test(url);
}