import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect } from 'react';
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