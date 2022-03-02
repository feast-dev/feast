"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getMatchingOptions = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var getSearchableLabel = function getSearchableLabel(option) {
  var normalize = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;
  var searchableLabel = option.searchableLabel || option.label;
  return normalize ? searchableLabel.trim().toLowerCase() : searchableLabel;
};

var getSelectedOptionForSearchValue = function getSelectedOptionForSearchValue(searchValue, selectedOptions) {
  var normalizedSearchValue = searchValue.toLowerCase();
  return selectedOptions.find(function (option) {
    return getSearchableLabel(option) === normalizedSearchValue;
  });
};

var collectMatchingOption = function collectMatchingOption(accumulator, option, normalizedSearchValue, isPreFiltered, selectedOptions) {
  // Don't show options that have already been requested if
  // the selectedOptions list exists
  if (selectedOptions) {
    var selectedOption = getSelectedOptionForSearchValue(getSearchableLabel(option, false), selectedOptions);

    if (selectedOption) {
      return false;
    }
  } // If the options have already been prefiltered then we can skip filtering against the search value.
  // TODO: I still don't quite understand how this works when hooked up to async


  if (isPreFiltered) {
    accumulator.push(option);
    return;
  }

  if (!normalizedSearchValue) {
    accumulator.push(option);
    return;
  }

  var normalizedOption = getSearchableLabel(option);

  if (normalizedOption.includes(normalizedSearchValue)) {
    accumulator.push(option);
  }
};

var getMatchingOptions = function getMatchingOptions(options, searchValue, isPreFiltered, selectedOptions) {
  var normalizedSearchValue = searchValue.toLowerCase();
  var matchingOptions = [];
  options.forEach(function (option) {
    collectMatchingOption(matchingOptions, option, normalizedSearchValue, isPreFiltered, selectedOptions);
  });
  return matchingOptions;
};

exports.getMatchingOptions = getMatchingOptions;