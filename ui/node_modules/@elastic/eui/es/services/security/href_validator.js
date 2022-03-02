/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import URL from 'url-parse';
export function validateHref(href) {
  // check href and treat it as invalid if it uses the javascript: protocol
  var parts = new URL(href); // eslint-disable-next-line no-script-url

  return parts.protocol !== 'javascript:';
}