import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var flattenOptionGroups = function flattenOptionGroups(optionsOrGroups) {
  return optionsOrGroups.reduce(function (options, optionOrGroup) {
    if (optionOrGroup.options) {
      options.push.apply(options, _toConsumableArray(optionOrGroup.options));
    } else {
      options.push(optionOrGroup);
    }

    return options;
  }, []);
};
export var getSelectedOptionForSearchValue = function getSelectedOptionForSearchValue(searchValue, selectedOptions, optionKey) {
  var normalizedSearchValue = searchValue.toLowerCase();
  return selectedOptions.find(function (option) {
    return option.label.toLowerCase() === normalizedSearchValue && (!optionKey || option.key === optionKey);
  });
};

var collectMatchingOption = function collectMatchingOption(accumulator, option, selectedOptions, normalizedSearchValue, isPreFiltered, showPrevSelected) {
  // Only show options which haven't yet been selected unless requested.
  var selectedOption = getSelectedOptionForSearchValue(option.label, selectedOptions, option.key);

  if (selectedOption && !showPrevSelected) {
    return false;
  } // If the options have already been pre-filtered then we can skip filtering against the search value.


  if (isPreFiltered) {
    accumulator.push(option);
    return;
  }

  if (!normalizedSearchValue) {
    accumulator.push(option);
    return;
  }

  var normalizedOption = option.label.trim().toLowerCase();

  if (normalizedOption.includes(normalizedSearchValue)) {
    accumulator.push(option);
  }
};

export var getMatchingOptions = function getMatchingOptions(options, selectedOptions, searchValue, isPreFiltered, showPrevSelected, sortMatchesBy) {
  var normalizedSearchValue = searchValue.trim().toLowerCase();
  var matchingOptions = [];
  options.forEach(function (option) {
    if (option.options) {
      var matchingOptionsForGroup = [];
      option.options.forEach(function (groupOption) {
        collectMatchingOption(matchingOptionsForGroup, groupOption, selectedOptions, normalizedSearchValue, isPreFiltered, showPrevSelected);
      });

      if (matchingOptionsForGroup.length > 0) {
        // Add option for group label
        matchingOptions.push({
          key: option.key,
          label: option.label,
          isGroupLabelOption: true
        }); // Add matching options for group

        matchingOptions.push.apply(matchingOptions, matchingOptionsForGroup);
      }
    } else {
      collectMatchingOption(matchingOptions, option, selectedOptions, normalizedSearchValue, isPreFiltered, showPrevSelected);
    }
  });

  if (sortMatchesBy === 'startsWith') {
    var refObj = {
      startWith: [],
      others: []
    };
    matchingOptions.forEach(function (object) {
      if (object.label.toLowerCase().startsWith(normalizedSearchValue)) {
        refObj.startWith.push(object);
      } else {
        refObj.others.push(object);
      }
    });
    return [].concat(_toConsumableArray(refObj.startWith), _toConsumableArray(refObj.others));
  }

  return matchingOptions;
};