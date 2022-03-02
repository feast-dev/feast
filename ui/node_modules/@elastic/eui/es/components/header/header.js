function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { EuiHeaderSectionItem, EuiHeaderSection } from './header_section';
import { EuiHeaderBreadcrumbs } from './header_breadcrumbs';
import { jsx as ___EmotionJSX } from "@emotion/react";

function createHeaderSection(sections, border) {
  return sections.map(function (section, index) {
    return ___EmotionJSX(EuiHeaderSectionItem, {
      key: index,
      border: border
    }, section);
  });
}

// Start a counter to manage the total number of fixed headers that need the body class
var euiHeaderFixedCounter = 0;
export var EuiHeader = function EuiHeader(_ref) {
  var children = _ref.children,
      className = _ref.className,
      sections = _ref.sections,
      _ref$position = _ref.position,
      position = _ref$position === void 0 ? 'static' : _ref$position,
      _ref$theme = _ref.theme,
      theme = _ref$theme === void 0 ? 'default' : _ref$theme,
      rest = _objectWithoutProperties(_ref, ["children", "className", "sections", "position", "theme"]);

  var classes = classNames('euiHeader', "euiHeader--".concat(theme), "euiHeader--".concat(position), className);
  useEffect(function () {
    if (position === 'fixed') {
      // Increment fixed header counter for each fixed header
      euiHeaderFixedCounter++;
      document.body.classList.add('euiBody--headerIsFixed');
      return function () {
        // Both decrement the fixed counter AND then check if there are none
        if (--euiHeaderFixedCounter === 0) {
          // If there are none, THEN remove class
          document.body.classList.remove('euiBody--headerIsFixed');
        }
      };
    }
  }, [position]);
  var contents;

  if (sections) {
    if (children) {
      // In case both children and sections are passed, warn in the console that the children will be disregarded
      console.warn('EuiHeader cannot accept both `children` and `sections`. It will disregard the `children`.');
    }

    contents = sections.map(function (section, index) {
      var content = [];

      if (section.items) {
        // Items get wrapped in EuiHeaderSection and each item in a EuiHeaderSectionItem
        content.push(___EmotionJSX(EuiHeaderSection, {
          key: "items-".concat(index)
        }, createHeaderSection(section.items, section.borders)));
      }

      if (section.breadcrumbs) {
        content.push( // Breadcrumbs are separate and cannot be contained in a EuiHeaderSection
        // in order for truncation to work
        ___EmotionJSX(EuiHeaderBreadcrumbs, _extends({
          key: "breadcrumbs-".concat(index),
          breadcrumbs: section.breadcrumbs
        }, section.breadcrumbProps)));
      }

      return content;
    });
  } else {
    contents = children;
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), contents);
};
EuiHeader.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * An array of objects to wrap in a #EuiHeaderSection.
       * Each section is spaced using `space-between`.
       * See #EuiHeaderSectionsProp for object details.
       * This prop disregards the prop `children` if both are passed.
       */
  sections: PropTypes.arrayOf(PropTypes.shape({
    /**
       * An arry of items that will be wrapped in a #EuiHeaderSectionItem
       */
    items: PropTypes.arrayOf(PropTypes.node.isRequired),

    /**
       * Apply the passed border side to each #EuiHeaderSectionItem
       */
    borders: PropTypes.oneOf(["left", "right", "none"]),

    /**
       * Breadcrumbs in the header cannot be wrapped in a #EuiHeaderSection in order for truncation to work.
       * Simply pass the array of EuiBreadcrumb objects
       */
    breadcrumbs: PropTypes.arrayOf(PropTypes.shape({
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string,

      /**
         * Visible label of the breadcrumb
         */
      text: PropTypes.node.isRequired,
      href: PropTypes.string,
      onClick: PropTypes.func,

      /**
         * Force a max-width on the breadcrumb text
         */
      truncate: PropTypes.bool,

      /**
         * Override the existing `aria-current` which defaults to `page` for the last breadcrumb
         */
      "aria-current": PropTypes.any
    }).isRequired),

    /**
       * Other props to pass to #EuiHeaderBreadcrumbs
       */
    breadcrumbProps: PropTypes.any
  }).isRequired),

  /**
       * Helper that positions the header against the window body and
       * adds the correct amount of top padding to the window when in `fixed` mode
       */
  position: PropTypes.oneOf(["static", "fixed"]),

  /**
       * The `default` will inherit its coloring from the light or dark theme.
       * Or, force the header into pseudo `dark` theme for all themes.
       */
  theme: PropTypes.oneOf(["default", "dark"])
};