"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.zIndex = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * NOTE: These were quick conversions of their Sass counterparts.
 *       They have yet to be used/tested.
 */
// Z-Index
// Remember that z-index is relative to parent and based on the stacking context.
// z-indexes only compete against other z-indexes when they exist as children of
// that shared parent.
// That means a popover with a settings of 2, will still show above a modal
// with a setting of 100, if it is within that modal and not besides it.
// Generally that means it's a good idea to consider things added to this file
// as competitive only as siblings.
// https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Positioning/Understanding_z_index/The_stacking_context
var zIndex = {
  level0: 0,
  level1: 1000,
  level2: 2000,
  level3: 3000,
  level4: 4000,
  level5: 5000,
  level6: 6000,
  level7: 7000,
  level8: 8000,
  level9: 9000 // --> These should be declared at the component level
  // content: computed(({ zIndex }) => zIndex.level0),
  // header: computed(({ zIndex }) => zIndex.level1),
  // contentMenu: computed(({ zIndex }) => zIndex.level2),
  // flyout: computed(({ zIndex }) => zIndex.level3),
  // navigation: computed(({ zIndex }) => zIndex.level4),
  // mask: computed(({ zIndex }) => zIndex.level6),
  // modal: computed(({ zIndex }) => zIndex.level8),
  // toastList: computed(({ zIndex }) => zIndex.level9),

};
exports.zIndex = zIndex;