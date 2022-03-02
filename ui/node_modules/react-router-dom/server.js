'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var React = require('react');
var history = require('history');
var reactRouterDom = require('react-router-dom');

/**
 * A <Router> that may not transition to any other location. This is useful
 * on the server where there is no stateful UI.
 */
function StaticRouter({
  basename,
  children,
  location: locationProp = "/"
}) {
  if (typeof locationProp === "string") {
    locationProp = history.parsePath(locationProp);
  }

  let action = history.Action.Pop;
  let location = {
    pathname: locationProp.pathname || "/",
    search: locationProp.search || "",
    hash: locationProp.hash || "",
    state: locationProp.state || null,
    key: locationProp.key || "default"
  };
  let staticNavigator = {
    createHref(to) {
      return typeof to === "string" ? to : history.createPath(to);
    },

    push(to) {
      throw new Error(`You cannot use navigator.push() on the server because it is a stateless ` + `environment. This error was probably triggered when you did a ` + `\`navigate(${JSON.stringify(to)})\` somewhere in your app.`);
    },

    replace(to) {
      throw new Error(`You cannot use navigator.replace() on the server because it is a stateless ` + `environment. This error was probably triggered when you did a ` + `\`navigate(${JSON.stringify(to)}, { replace: true })\` somewhere ` + `in your app.`);
    },

    go(delta) {
      throw new Error(`You cannot use navigator.go() on the server because it is a stateless ` + `environment. This error was probably triggered when you did a ` + `\`navigate(${delta})\` somewhere in your app.`);
    },

    back() {
      throw new Error(`You cannot use navigator.back() on the server because it is a stateless ` + `environment.`);
    },

    forward() {
      throw new Error(`You cannot use navigator.forward() on the server because it is a stateless ` + `environment.`);
    }

  };
  return /*#__PURE__*/React.createElement(reactRouterDom.Router, {
    basename: basename,
    children: children,
    location: location,
    navigationType: action,
    navigator: staticNavigator,
    static: true
  });
}

exports.StaticRouter = StaticRouter;
