/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Lowercases input and replaces spaces with hyphens:
 * e.g. 'GridView Example' -> 'gridview-example'
 *
 * @param {string} string The starting string
 * @returns {string} Lowercase, dashed version of the starting staring
 */
export function slugify(str) {
  // Calculate the number of initials to show, maxing out at MAX_INITIALS
  var parts = str.toLowerCase().replace(/[-]+/g, ' ').replace(/[^\w^\s]+/g, '').replace(/ +/g, ' ').split(' ');
  return parts.join('-');
}