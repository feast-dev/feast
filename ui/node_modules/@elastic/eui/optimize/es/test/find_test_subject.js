/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Find node which matches a specific test subject selector. Returns ReactWrappers around DOM element,
 * https://github.com/airbnb/enzyme/tree/master/docs/api/ReactWrapper.
 * Common use cases include calling simulate or getDOMNode on the returned ReactWrapper.
 *
 * The ~= matcher looks for the value in space-separated list, allowing support for multiple data-test-subj
 * values on a single element. See https://www.w3.org/TR/selectors-3/#attribute-selectors for more
 * info on the other possible matchers.
 */
var MATCHERS = ['=', // Exact match
'~=', // Exists in a space-separated list
'|=', // Begins with substring, followed by '-'
'^=', // Begins with substring
'$=', // Ends with substring
'*=' // Contains substring
];
export var findTestSubject = function findTestSubject(mountedComponent, testSubjectSelector) {
  var matcher = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : '~=';

  if (!MATCHERS.includes(matcher)) {
    throw new Error("Matcher ".concat(matcher, " not found in list of allowed matchers: ").concat(MATCHERS.join(' ')));
  }

  var testSubject = mountedComponent.find("[data-test-subj".concat(matcher, "\"").concat(testSubjectSelector, "\"]")); // Restores Enzyme 2's find behavior, which was to only return ReactWrappers around DOM elements.
  // Enzyme 3 returns ReactWrappers around both DOM elements and React components.
  // https://github.com/airbnb/enzyme/issues/1174

  return testSubject.hostNodes();
};