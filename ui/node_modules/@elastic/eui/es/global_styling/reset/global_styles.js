function _objectDestructuringEmpty(obj) { if (obj == null) throw new TypeError("Cannot destructure undefined"); }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { Global, css } from '@emotion/react';
import { useScrollBar } from '../mixins/_helpers';
import { shade, tint, transparentize } from '../../services/color';
import { useEuiTheme } from '../../services/theme';
import { resetStyles as reset } from './reset';
import { isLegacyTheme } from '../../themes';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiGlobalStyles = function EuiGlobalStyles(_ref) {
  _objectDestructuringEmpty(_ref);

  var _useEuiTheme = useEuiTheme(),
      _useEuiTheme$euiTheme = _useEuiTheme.euiTheme,
      base = _useEuiTheme$euiTheme.base,
      border = _useEuiTheme$euiTheme.border,
      colors = _useEuiTheme$euiTheme.colors,
      font = _useEuiTheme$euiTheme.font,
      themeName = _useEuiTheme$euiTheme.themeName,
      colorMode = _useEuiTheme.colorMode;
  /**
   * Declaring the top level scrollbar colors to match the theme also requires setting the sizes on Chrome
   * so that it knows to use custom styles. Therefore, we just reuse the same scrollbar mixin with thick size.
   */


  var scrollbarStyles = useScrollBar({
    trackColor: colorMode === 'LIGHT' ? shade(colors.body, 0.03) : tint(colors.body, 0.07),
    width: 'auto'
  });
  /**
   * Early return with no styles if using the legacy theme,
   * which has reset and global styles included in the compiled CSS.
   * Comes after `scrollbarStyles` because of hook rules.
   */

  if (isLegacyTheme(themeName)) {
    return null;
  }
  /**
   * This font reset sets all our base font/typography related properties
   * that are needed to override browser-specific element settings.
   */


  var fontReset = "\n    font-family: ".concat(font.family, ";\n    font-size: ", "".concat(font.scale[font.body.scale] * base, "px"), ";\n    line-height: ").concat(base / (font.scale[font.body.scale] * base), ";\n    font-weight: ").concat(font.weight[font.body.weight], ";\n    ").concat(font.body.letterSpacing ? "letter-spacing: ".concat(font.body.letterSpacing, ";") : '', "\n  ");
  /**
   * Outline/Focus state resets
   */

  var focusReset = function focusReset() {
    // The latest theme utilizes `focus-visible` to turn on focus outlines.
    // But this is browser-dependend:
    // 👉 Safari and Firefox innately respect only showing the outline with keyboard only
    // 💔 But they don't allow coloring of the 'auto'/default outline, so contrast is no good in dark mode.
    // 👉 For these browsers we use the solid type in order to match with \`currentColor\`.
    // 😦 Which does means the outline will be square
    return "*:focus {\n      outline: currentColor solid ".concat(border.width.thick, ";\n      outline-offset: calc(-(").concat(border.width.thick, " / 2) * -1);\n\n      // \uD83D\uDC40 Chrome respects :focus-visible and allows coloring the `auto` style\n      &:focus-visible {\n        outline-style: auto;\n      }\n\n      // \uD83D\uDE45\u200D\u2640\uFE0F But Chrome also needs to have the outline forcefully removed from regular `:focus` state\n      &:not(:focus-visible) {\n        outline: none;\n      }\n    }\n\n    // Dark mode's highlighted doesn't work well so lets just set it the same as our focus background\n    ::selection {\n      background: ").concat(transparentize(colors.primary, colorMode === 'LIGHT' ? 0.1 : 0.2), "\n    }");
  };
  /**
   * Final styles
   */


  var styles = /*#__PURE__*/css(reset, " html{", scrollbarStyles, " ", fontReset, " text-size-adjust:100%;font-kerning:normal;height:100%;background-color:", colors.body, ";color:", colors.text, ";}code,pre,kbd,samp{font-family:", font.familyCode, ";}input,textarea,select{", fontReset, ";}button{font-family:", font.family, ";}em{font-style:italic;}strong{font-weight:", font.weight.bold, ";}", focusReset(), " a{color:", colors.primaryText, ";&,&:hover,&:focus{text-decoration:none;}}" + (process.env.NODE_ENV === "production" ? "" : ";label:global_styles-styles;"), process.env.NODE_ENV === "production" ? "" : "/*# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIi4uLy4uLy4uL3NyYy9nbG9iYWxfc3R5bGluZy9yZXNldC9nbG9iYWxfc3R5bGVzLnRzeCJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFrR29CIiwiZmlsZSI6Ii4uLy4uLy4uL3NyYy9nbG9iYWxfc3R5bGluZy9yZXNldC9nbG9iYWxfc3R5bGVzLnRzeCIsInNvdXJjZXNDb250ZW50IjpbIi8qXG4gKiBDb3B5cmlnaHQgRWxhc3RpY3NlYXJjaCBCLlYuIGFuZC9vciBsaWNlbnNlZCB0byBFbGFzdGljc2VhcmNoIEIuVi4gdW5kZXIgb25lXG4gKiBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gTGljZW5zZWQgdW5kZXIgdGhlIEVsYXN0aWMgTGljZW5zZVxuICogMi4wIGFuZCB0aGUgU2VydmVyIFNpZGUgUHVibGljIExpY2Vuc2UsIHYgMTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHRcbiAqIGluIGNvbXBsaWFuY2Ugd2l0aCwgYXQgeW91ciBlbGVjdGlvbiwgdGhlIEVsYXN0aWMgTGljZW5zZSAyLjAgb3IgdGhlIFNlcnZlclxuICogU2lkZSBQdWJsaWMgTGljZW5zZSwgdiAxLlxuICovXG5cbmltcG9ydCBSZWFjdCBmcm9tICdyZWFjdCc7XG5pbXBvcnQgeyBHbG9iYWwsIGNzcyB9IGZyb20gJ0BlbW90aW9uL3JlYWN0JztcbmltcG9ydCB7IHVzZVNjcm9sbEJhciB9IGZyb20gJy4uL21peGlucy9faGVscGVycyc7XG5pbXBvcnQgeyBzaGFkZSwgdGludCwgdHJhbnNwYXJlbnRpemUgfSBmcm9tICcuLi8uLi9zZXJ2aWNlcy9jb2xvcic7XG5pbXBvcnQgeyB1c2VFdWlUaGVtZSB9IGZyb20gJy4uLy4uL3NlcnZpY2VzL3RoZW1lJztcbmltcG9ydCB7IHJlc2V0U3R5bGVzIGFzIHJlc2V0IH0gZnJvbSAnLi9yZXNldCc7XG5pbXBvcnQgeyBpc0xlZ2FjeVRoZW1lIH0gZnJvbSAnLi4vLi4vdGhlbWVzJztcblxuZXhwb3J0IGludGVyZmFjZSBFdWlHbG9iYWxTdHlsZXNQcm9wcyB7fVxuXG5leHBvcnQgY29uc3QgRXVpR2xvYmFsU3R5bGVzID0gKHt9OiBFdWlHbG9iYWxTdHlsZXNQcm9wcykgPT4ge1xuICBjb25zdCB7XG4gICAgZXVpVGhlbWU6IHsgYmFzZSwgYm9yZGVyLCBjb2xvcnMsIGZvbnQsIHRoZW1lTmFtZSB9LFxuICAgIGNvbG9yTW9kZSxcbiAgfSA9IHVzZUV1aVRoZW1lKCk7XG5cbiAgLyoqXG4gICAqIERlY2xhcmluZyB0aGUgdG9wIGxldmVsIHNjcm9sbGJhciBjb2xvcnMgdG8gbWF0Y2ggdGhlIHRoZW1lIGFsc28gcmVxdWlyZXMgc2V0dGluZyB0aGUgc2l6ZXMgb24gQ2hyb21lXG4gICAqIHNvIHRoYXQgaXQga25vd3MgdG8gdXNlIGN1c3RvbSBzdHlsZXMuIFRoZXJlZm9yZSwgd2UganVzdCByZXVzZSB0aGUgc2FtZSBzY3JvbGxiYXIgbWl4aW4gd2l0aCB0aGljayBzaXplLlxuICAgKi9cbiAgY29uc3Qgc2Nyb2xsYmFyU3R5bGVzID0gdXNlU2Nyb2xsQmFyKHtcbiAgICB0cmFja0NvbG9yOlxuICAgICAgY29sb3JNb2RlID09PSAnTElHSFQnXG4gICAgICAgID8gc2hhZGUoY29sb3JzLmJvZHksIDAuMDMpXG4gICAgICAgIDogdGludChjb2xvcnMuYm9keSwgMC4wNyksXG4gICAgd2lkdGg6ICdhdXRvJyxcbiAgfSk7XG5cbiAgLyoqXG4gICAqIEVhcmx5IHJldHVybiB3aXRoIG5vIHN0eWxlcyBpZiB1c2luZyB0aGUgbGVnYWN5IHRoZW1lLFxuICAgKiB3aGljaCBoYXMgcmVzZXQgYW5kIGdsb2JhbCBzdHlsZXMgaW5jbHVkZWQgaW4gdGhlIGNvbXBpbGVkIENTUy5cbiAgICogQ29tZXMgYWZ0ZXIgYHNjcm9sbGJhclN0eWxlc2AgYmVjYXVzZSBvZiBob29rIHJ1bGVzLlxuICAgKi9cbiAgaWYgKGlzTGVnYWN5VGhlbWUodGhlbWVOYW1lKSkge1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgLyoqXG4gICAqIFRoaXMgZm9udCByZXNldCBzZXRzIGFsbCBvdXIgYmFzZSBmb250L3R5cG9ncmFwaHkgcmVsYXRlZCBwcm9wZXJ0aWVzXG4gICAqIHRoYXQgYXJlIG5lZWRlZCB0byBvdmVycmlkZSBicm93c2VyLXNwZWNpZmljIGVsZW1lbnQgc2V0dGluZ3MuXG4gICAqL1xuICBjb25zdCBmb250UmVzZXQgPSBgXG4gICAgZm9udC1mYW1pbHk6ICR7Zm9udC5mYW1pbHl9O1xuICAgIGZvbnQtc2l6ZTogJHtgJHtmb250LnNjYWxlW2ZvbnQuYm9keS5zY2FsZV0gKiBiYXNlfXB4YH07XG4gICAgbGluZS1oZWlnaHQ6ICR7YmFzZSAvIChmb250LnNjYWxlW2ZvbnQuYm9keS5zY2FsZV0gKiBiYXNlKX07XG4gICAgZm9udC13ZWlnaHQ6ICR7Zm9udC53ZWlnaHRbZm9udC5ib2R5LndlaWdodF19O1xuICAgICR7XG4gICAgICBmb250LmJvZHkubGV0dGVyU3BhY2luZ1xuICAgICAgICA/IGBsZXR0ZXItc3BhY2luZzogJHtmb250LmJvZHkubGV0dGVyU3BhY2luZ307YFxuICAgICAgICA6ICcnXG4gICAgfVxuICBgO1xuXG4gIC8qKlxuICAgKiBPdXRsaW5lL0ZvY3VzIHN0YXRlIHJlc2V0c1xuICAgKi9cbiAgY29uc3QgZm9jdXNSZXNldCA9ICgpID0+IHtcbiAgICAvLyBUaGUgbGF0ZXN0IHRoZW1lIHV0aWxpemVzIGBmb2N1cy12aXNpYmxlYCB0byB0dXJuIG9uIGZvY3VzIG91dGxpbmVzLlxuICAgIC8vIEJ1dCB0aGlzIGlzIGJyb3dzZXItZGVwZW5kZW5kOlxuICAgIC8vIPCfkYkgU2FmYXJpIGFuZCBGaXJlZm94IGlubmF0ZWx5IHJlc3BlY3Qgb25seSBzaG93aW5nIHRoZSBvdXRsaW5lIHdpdGgga2V5Ym9hcmQgb25seVxuICAgIC8vIPCfkpQgQnV0IHRoZXkgZG9uJ3QgYWxsb3cgY29sb3Jpbmcgb2YgdGhlICdhdXRvJy9kZWZhdWx0IG91dGxpbmUsIHNvIGNvbnRyYXN0IGlzIG5vIGdvb2QgaW4gZGFyayBtb2RlLlxuICAgIC8vIPCfkYkgRm9yIHRoZXNlIGJyb3dzZXJzIHdlIHVzZSB0aGUgc29saWQgdHlwZSBpbiBvcmRlciB0byBtYXRjaCB3aXRoIFxcYGN1cnJlbnRDb2xvclxcYC5cbiAgICAvLyDwn5imIFdoaWNoIGRvZXMgbWVhbnMgdGhlIG91dGxpbmUgd2lsbCBiZSBzcXVhcmVcbiAgICByZXR1cm4gYCo6Zm9jdXMge1xuICAgICAgb3V0bGluZTogY3VycmVudENvbG9yIHNvbGlkICR7Ym9yZGVyLndpZHRoLnRoaWNrfTtcbiAgICAgIG91dGxpbmUtb2Zmc2V0OiBjYWxjKC0oJHtib3JkZXIud2lkdGgudGhpY2t9IC8gMikgKiAtMSk7XG5cbiAgICAgIC8vIPCfkYAgQ2hyb21lIHJlc3BlY3RzIDpmb2N1cy12aXNpYmxlIGFuZCBhbGxvd3MgY29sb3JpbmcgdGhlIFxcYGF1dG9cXGAgc3R5bGVcbiAgICAgICY6Zm9jdXMtdmlzaWJsZSB7XG4gICAgICAgIG91dGxpbmUtc3R5bGU6IGF1dG87XG4gICAgICB9XG5cbiAgICAgIC8vIPCfmYXigI3imYDvuI8gQnV0IENocm9tZSBhbHNvIG5lZWRzIHRvIGhhdmUgdGhlIG91dGxpbmUgZm9yY2VmdWxseSByZW1vdmVkIGZyb20gcmVndWxhciBcXGA6Zm9jdXNcXGAgc3RhdGVcbiAgICAgICY6bm90KDpmb2N1cy12aXNpYmxlKSB7XG4gICAgICAgIG91dGxpbmU6IG5vbmU7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gRGFyayBtb2RlJ3MgaGlnaGxpZ2h0ZWQgZG9lc24ndCB3b3JrIHdlbGwgc28gbGV0cyBqdXN0IHNldCBpdCB0aGUgc2FtZSBhcyBvdXIgZm9jdXMgYmFja2dyb3VuZFxuICAgIDo6c2VsZWN0aW9uIHtcbiAgICAgIGJhY2tncm91bmQ6ICR7dHJhbnNwYXJlbnRpemUoXG4gICAgICAgIGNvbG9ycy5wcmltYXJ5LFxuICAgICAgICBjb2xvck1vZGUgPT09ICdMSUdIVCcgPyAwLjEgOiAwLjJcbiAgICAgICl9XG4gICAgfWA7XG4gIH07XG5cbiAgLyoqXG4gICAqIEZpbmFsIHN0eWxlc1xuICAgKi9cbiAgY29uc3Qgc3R5bGVzID0gY3NzYFxuICAgICR7cmVzZXR9XG5cbiAgICBodG1sIHtcbiAgICAgICR7c2Nyb2xsYmFyU3R5bGVzfVxuICAgICAgJHtmb250UmVzZXR9XG4gICAgICB0ZXh0LXNpemUtYWRqdXN0OiAxMDAlO1xuICAgICAgZm9udC1rZXJuaW5nOiBub3JtYWw7XG4gICAgICBoZWlnaHQ6IDEwMCU7XG4gICAgICBiYWNrZ3JvdW5kLWNvbG9yOiAke2NvbG9ycy5ib2R5fTtcbiAgICAgIGNvbG9yOiAke2NvbG9ycy50ZXh0fTtcbiAgICB9XG5cbiAgICBjb2RlLFxuICAgIHByZSxcbiAgICBrYmQsXG4gICAgc2FtcCB7XG4gICAgICBmb250LWZhbWlseTogJHtmb250LmZhbWlseUNvZGV9O1xuICAgIH1cblxuICAgIGlucHV0LFxuICAgIHRleHRhcmVhLFxuICAgIHNlbGVjdCB7XG4gICAgICAke2ZvbnRSZXNldH1cbiAgICB9XG5cbiAgICBidXR0b24ge1xuICAgICAgZm9udC1mYW1pbHk6ICR7Zm9udC5mYW1pbHl9O1xuICAgIH1cblxuICAgIGVtIHtcbiAgICAgIGZvbnQtc3R5bGU6IGl0YWxpYztcbiAgICB9XG5cbiAgICBzdHJvbmcge1xuICAgICAgZm9udC13ZWlnaHQ6ICR7Zm9udC53ZWlnaHQuYm9sZH07XG4gICAgfVxuXG4gICAgJHtmb2N1c1Jlc2V0KCl9XG5cbiAgICBhIHtcbiAgICAgIGNvbG9yOiAke2NvbG9ycy5wcmltYXJ5VGV4dH07XG5cbiAgICAgICYsXG4gICAgICAmOmhvdmVyLFxuICAgICAgJjpmb2N1cyB7XG4gICAgICAgIHRleHQtZGVjb3JhdGlvbjogbm9uZTtcbiAgICAgIH1cbiAgICB9XG4gIGA7XG5cbiAgcmV0dXJuIDxHbG9iYWwgc3R5bGVzPXtzdHlsZXN9IC8+O1xufTtcbiJdfQ== */");
  return ___EmotionJSX(Global, {
    styles: styles
  });
};