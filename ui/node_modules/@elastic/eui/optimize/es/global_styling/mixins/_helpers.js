/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import chroma from 'chroma-js';
import { useEuiTheme } from '../../services/theme/hooks';
import { transparentize } from '../../services/color';
import { useOverflowShadow } from './_shadow';

/**
 * Set scroll bar appearance on Chrome (and firefox).
 * All parameters are optional and default to specific global settings.
 */
export var useScrollBar = function useScrollBar() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      _thumbColor = _ref.thumbColor,
      _ref$trackColor = _ref.trackColor,
      trackColor = _ref$trackColor === void 0 ? 'transparent' : _ref$trackColor,
      _ref$width = _ref.width,
      width = _ref$width === void 0 ? 'thin' : _ref$width,
      _size = _ref.size,
      _corner = _ref.corner;

  var _useEuiTheme = useEuiTheme(),
      _useEuiTheme$euiTheme = _useEuiTheme.euiTheme,
      colors = _useEuiTheme$euiTheme.colors,
      size = _useEuiTheme$euiTheme.size; // Set defaults from theme


  var thumbColor = _thumbColor || transparentize(colors.darkShade, 0.5);

  var scrollBarSize = _size || size.base;
  var scrollBarCorner = _corner || width === 'thin' ? "calc(".concat(size.s, " * 0.75)") : size.xs; // Firefox's scrollbar coloring cascades, but the sizing does not,
  // so it's being added to this mixin for allowing support wherever custom scrollbars are

  var firefoxSupport = "scrollbar-color: ".concat(thumbColor, " ").concat(trackColor, ";");
  return "scrollbar-width: ".concat(width, ";\n\n    &::-webkit-scrollbar {\n      width: ").concat(scrollBarSize, ";\n      height: ").concat(scrollBarSize, ";\n    }\n\n    &::-webkit-scrollbar-thumb {\n      background-color: ").concat(thumbColor, ";\n      background-clip: content-box;\n      border-radius: ").concat(scrollBarSize, ";\n      border: ").concat(scrollBarCorner, " solid ").concat(trackColor, ";\n    }\n\n    &::-webkit-scrollbar-corner,\n    &::-webkit-scrollbar-track {\n      background-color: ").concat(trackColor, ";\n    }\n\n    ").concat(firefoxSupport, "\n  ");
};
/**
 * NOTE: The ones below this comment were quick conversions of their Sass counterparts.
 *       They have yet to be used/tested.
 */
// Useful border shade when dealing with images of unknown color.

export var useInnerBorder = function useInnerBorder(_ref2) {
  var _ref2$type = _ref2.type,
      type = _ref2$type === void 0 ? 'dark' : _ref2$type,
      _ref2$borderRadius = _ref2.borderRadius,
      borderRadius = _ref2$borderRadius === void 0 ? 0 : _ref2$borderRadius,
      _ref2$alpha = _ref2.alpha,
      alpha = _ref2$alpha === void 0 ? 0.1 : _ref2$alpha;

  var _useEuiTheme2 = useEuiTheme(),
      colors = _useEuiTheme2.euiTheme.colors;

  var color = chroma(type === 'dark' ? colors.darkestShade : colors.emptyShade).alpha(alpha).css();
  return "\n    position: relative;\n\n    &:after {\n      position: absolute;\n      top: 0;\n      left: 0;\n      right: 0;\n      bottom: 0;\n      border-radius: ".concat(borderRadius, ";\n      content: '';\n      pointer-events: none;\n      border: 1px solid ").concat(color, ";\n    }\n  ");
};
/**
 * 1. Focus rings shouldn't be visible on scrollable regions, but a11y requires them to be focusable.
 *    Browser's supporting `:focus-visible` will still show outline on keyboard focus only.
 *    Others like Safari, won't show anything at all.
 */
// Just overflow and scrollbars

export var useYScroll = function useYScroll() {
  return "\n  ".concat(useScrollBar(), "\n  height: 100%;\n  overflow-y: auto;\n  overflow-x: hidden;\n  &:focus {\n    outline: none; /* 1 */\n  }\n");
};
export var useXScroll = function useXScroll() {
  return "\n  ".concat(useScrollBar(), "\n  overflow-x: auto;\n\n  &:focus {\n    outline: none; /* 1 */\n  }\n");
}; // // The full overflow with shadow

export var useYScrollWithShadows = function useYScrollWithShadows() {
  return "\n  ".concat(useYScroll(), "\n  ").concat(useOverflowShadow({
    direction: 'y'
  }), "\n");
};
export var useXScrollWithShadows = function useXScrollWithShadows() {
  return "\n  ".concat(useXScroll(), "\n  ").concat(useOverflowShadow({
    direction: 'x'
  }), "\n");
}; // Hiding elements offscreen to only be read by screen reader

export var useScreenReaderOnly = function useScreenReaderOnly() {
  return "\n  position: absolute;\n  left: -10000px;\n  top: auto;\n  width: 1px;\n  height: 1px;\n  overflow: hidden;\n";
}; // Doesn't have reduced motion turned on

export var useCanAnimate = function useCanAnimate(content) {
  return "\n  @media screen and (prefers-reduced-motion: no-preference) {\n    ".concat(content, "\n  }\n");
};