import React from 'react';

var getItem = function getItem(key) {
  try {
    var itemValue = localStorage.getItem(key);

    if (typeof itemValue === 'string') {
      return JSON.parse(itemValue);
    }

    return undefined;
  } catch (_unused) {
    return undefined;
  }
};

export default function useLocalStorage(key, defaultValue) {
  var _React$useState = React.useState(),
      value = _React$useState[0],
      setValue = _React$useState[1];

  React.useEffect(function () {
    var initialValue = getItem(key);

    if (typeof initialValue === 'undefined' || initialValue === null) {
      setValue(typeof defaultValue === 'function' ? defaultValue() : defaultValue);
    } else {
      setValue(initialValue);
    }
  }, [defaultValue, key]);
  var setter = React.useCallback(function (updater) {
    setValue(function (old) {
      var newVal = updater;

      if (typeof updater == 'function') {
        newVal = updater(old);
      }

      try {
        localStorage.setItem(key, JSON.stringify(newVal));
      } catch (_unused2) {}

      return newVal;
    });
  }, [key]);
  return [value, setter];
}