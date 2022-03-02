import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
export var defaultTheme = {
  background: '#0b1521',
  backgroundAlt: '#132337',
  foreground: 'white',
  gray: '#3f4e60',
  grayAlt: '#222e3e',
  inputBackgroundColor: '#fff',
  inputTextColor: '#000',
  success: '#00ab52',
  danger: '#ff0085',
  active: '#006bff',
  warning: '#ffb200'
};
var ThemeContext = /*#__PURE__*/React.createContext(defaultTheme);
export function ThemeProvider(_ref) {
  var theme = _ref.theme,
      rest = _objectWithoutPropertiesLoose(_ref, ["theme"]);

  return /*#__PURE__*/React.createElement(ThemeContext.Provider, _extends({
    value: theme
  }, rest));
}
export function useTheme() {
  return React.useContext(ThemeContext);
}