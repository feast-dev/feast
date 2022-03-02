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
import React, { Fragment } from 'react';
import PropTypes from "prop-types";
import { EuiMark } from '../mark';
import { jsx as ___EmotionJSX } from "@emotion/react";

var highlight = function highlight(searchSubject, searchValue, isStrict, highlightAll) {
  if (!searchValue) {
    return searchSubject;
  }

  if (!searchSubject) {
    return null;
  }

  if (highlightAll) {
    var chunks = getHightlightWords(searchSubject, searchValue, isStrict);
    return ___EmotionJSX(Fragment, null, chunks.map(function (chunk) {
      var end = chunk.end,
          highlight = chunk.highlight,
          start = chunk.start;
      var value = searchSubject.substr(start, end - start);

      if (highlight) {
        return ___EmotionJSX(EuiMark, {
          key: start
        }, value);
      }

      return value;
    }));
  }

  var normalizedSearchSubject = isStrict ? searchSubject : searchSubject.toLowerCase();
  var normalizedSearchValue = isStrict ? searchValue : searchValue.toLowerCase();
  var indexOfMatch = normalizedSearchSubject.indexOf(normalizedSearchValue);

  if (indexOfMatch === -1) {
    return searchSubject;
  }

  var preMatch = searchSubject.substr(0, indexOfMatch);
  var match = searchSubject.substr(indexOfMatch, searchValue.length);
  var postMatch = searchSubject.substr(indexOfMatch + searchValue.length);
  return ___EmotionJSX(Fragment, null, preMatch, ___EmotionJSX(EuiMark, null, match), postMatch);
};

var getHightlightWords = function getHightlightWords(searchSubject, searchValue, isStrict) {
  var regex = new RegExp(searchValue, isStrict ? 'g' : 'gi');
  var matches = [];
  var match;

  while ((match = regex.exec(searchSubject)) !== null) {
    matches.push({
      start: match.index,
      end: (match.index || 0) + match[0].length
    });
  }

  return fillInChunks(matches, searchSubject.length);
};

var fillInChunks = function fillInChunks(chunksToHighlight, totalLength) {
  var allChunks = [];

  var append = function append(start, end, highlight) {
    if (end - start > 0) {
      allChunks.push({
        start: start,
        end: end,
        highlight: highlight
      });
    }
  };

  if (chunksToHighlight.length === 0) {
    append(0, totalLength, false);
  } else {
    var lastIndex = 0;
    chunksToHighlight.forEach(function (chunk) {
      append(lastIndex, chunk.start, false);
      append(chunk.start, chunk.end, true);
      lastIndex = chunk.end;
    });
    append(lastIndex, totalLength, false);
  }

  return allChunks;
};

export var EuiHighlight = function EuiHighlight(_ref) {
  var children = _ref.children,
      className = _ref.className,
      search = _ref.search,
      _ref$strict = _ref.strict,
      strict = _ref$strict === void 0 ? false : _ref$strict,
      _ref$highlightAll = _ref.highlightAll,
      highlightAll = _ref$highlightAll === void 0 ? false : _ref$highlightAll,
      rest = _objectWithoutProperties(_ref, ["children", "className", "search", "strict", "highlightAll"]);

  return ___EmotionJSX("span", _extends({
    className: className
  }, rest), highlight(children, search, strict, highlightAll));
};
EuiHighlight.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * string to highlight as this component's content
       */
  children: PropTypes.string.isRequired,

  /**
       * What to search for
       */
  search: PropTypes.string.isRequired,

  /**
       * Should the search be strict or not
       */
  strict: PropTypes.bool,

  /**
       * Should highlight all matches
       */
  highlightAll: PropTypes.bool
};