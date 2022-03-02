import React from 'react'; // CONTEXT

function createValue() {
  var _isReset = false;
  return {
    clearReset: function clearReset() {
      _isReset = false;
    },
    reset: function reset() {
      _isReset = true;
    },
    isReset: function isReset() {
      return _isReset;
    }
  };
}

var QueryErrorResetBoundaryContext = /*#__PURE__*/React.createContext(createValue()); // HOOK

export var useQueryErrorResetBoundary = function useQueryErrorResetBoundary() {
  return React.useContext(QueryErrorResetBoundaryContext);
}; // COMPONENT

export var QueryErrorResetBoundary = function QueryErrorResetBoundary(_ref) {
  var children = _ref.children;
  var value = React.useMemo(function () {
    return createValue();
  }, []);
  return /*#__PURE__*/React.createElement(QueryErrorResetBoundaryContext.Provider, {
    value: value
  }, typeof children === 'function' ? children(value) : children);
};