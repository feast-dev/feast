function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

import PropTypes from "prop-types";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import { isString } from '../../services/predicate';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { EuiSearchBox } from './search_box';
import { EuiSearchFilters } from './search_filters';
import { Query } from './query';
import { jsx as ___EmotionJSX } from "@emotion/react";
export { Query, AST as Ast } from './query';

var parseQuery = function parseQuery(query, props) {
  var schema = undefined;

  if (props.box && props.box.schema && _typeof(props.box.schema) === 'object') {
    schema = props.box.schema;
  }

  var dateFormat = props.dateFormat;
  var parseOptions = {
    schema: schema,
    dateFormat: dateFormat
  };

  if (!query) {
    return Query.parse('', parseOptions);
  }

  return isString(query) ? Query.parse(query, parseOptions) : query;
};

export var EuiSearchBar = /*#__PURE__*/function (_Component) {
  _inherits(EuiSearchBar, _Component);

  var _super = _createSuper(EuiSearchBar);

  function EuiSearchBar(props) {
    var _this;

    _classCallCheck(this, EuiSearchBar);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "onSearch", function (queryText) {
      try {
        var query = parseQuery(queryText, _this.props);

        _this.notifyControllingParent({
          query: query,
          queryText: queryText,
          error: null
        });

        _this.setState({
          query: query,
          queryText: queryText,
          error: null
        });
      } catch (e) {
        var error = {
          name: e.name,
          message: e.message
        };

        _this.notifyControllingParent({
          query: null,
          queryText: queryText,
          error: error
        });

        _this.setState({
          queryText: queryText,
          error: error
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onFiltersChange", function (query) {
      _this.notifyControllingParent({
        query: query,
        queryText: query.text,
        error: null
      });

      _this.setState({
        query: query,
        queryText: query.text,
        error: null
      });
    });

    var _query = parseQuery(props.defaultQuery || props.query, props);

    _this.state = {
      query: _query,
      queryText: _query.text,
      error: null
    };
    return _this;
  }

  _createClass(EuiSearchBar, [{
    key: "notifyControllingParent",
    value: function notifyControllingParent(newState) {
      var onChange = this.props.onChange;

      if (!onChange) {
        return;
      }

      var oldState = this.state;
      var query = newState.query,
          queryText = newState.queryText,
          error = newState.error;
      var isQueryDifferent = oldState.queryText !== queryText;
      var oldError = oldState.error ? oldState.error.message : null;
      var newError = error ? error.message : null;
      var isErrorDifferent = oldError !== newError;

      if (isQueryDifferent || isErrorDifferent) {
        if (error == null) {
          onChange({
            query: query,
            queryText: queryText,
            error: error
          });
        } else {
          onChange({
            query: null,
            queryText: queryText,
            error: error
          });
        }
      }
    }
  }, {
    key: "renderTools",
    value: function renderTools(tools) {
      if (!tools) {
        return undefined;
      }

      if (Array.isArray(tools)) {
        return tools.map(function (tool) {
          return ___EmotionJSX(EuiFlexItem, {
            grow: false,
            key: tool.key == null ? undefined : tool.key
          }, tool);
        });
      }

      return ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, tools);
    }
  }, {
    key: "render",
    value: function render() {
      var _this$state = this.state,
          query = _this$state.query,
          queryText = _this$state.queryText,
          error = _this$state.error;
      var _this$props = this.props,
          _this$props$box = _this$props.box;
      _this$props$box = _this$props$box === void 0 ? {
        schema: ''
      } : _this$props$box;

      var schema = _this$props$box.schema,
          box = _objectWithoutProperties(_this$props$box, ["schema"]),
          filters = _this$props.filters,
          toolsLeft = _this$props.toolsLeft,
          toolsRight = _this$props.toolsRight;

      var toolsLeftEl = this.renderTools(toolsLeft);
      var filtersBar = !filters ? undefined : ___EmotionJSX(EuiFlexItem, {
        className: "euiSearchBar__filtersHolder",
        grow: false
      }, ___EmotionJSX(EuiSearchFilters, {
        filters: filters,
        query: query,
        onChange: this.onFiltersChange
      }));
      var toolsRightEl = this.renderTools(toolsRight);
      return ___EmotionJSX(EuiFlexGroup, {
        gutterSize: "m",
        alignItems: "center",
        wrap: true
      }, toolsLeftEl, ___EmotionJSX(EuiFlexItem, {
        className: "euiSearchBar__searchHolder",
        grow: true
      }, ___EmotionJSX(EuiSearchBox, _extends({}, box, {
        query: queryText,
        onSearch: this.onSearch,
        isInvalid: error != null,
        title: error ? error.message : undefined
      }))), filtersBar, toolsRightEl);
    }
  }], [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      if ((nextProps.query || nextProps.query === '') && (!prevState.query || typeof nextProps.query !== 'string' && nextProps.query.text !== prevState.query.text || typeof nextProps.query === 'string' && nextProps.query !== prevState.query.text)) {
        var query = parseQuery(nextProps.query, nextProps);
        return {
          query: query,
          queryText: query.text,
          error: null
        };
      }

      return null;
    }
  }]);

  return EuiSearchBar;
}(Component);

_defineProperty(EuiSearchBar, "Query", Query);

EuiSearchBar.propTypes = {
  onChange: PropTypes.func,

  /**
     The initial query the bar will hold when first mounted
     */
  defaultQuery: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.string.isRequired]),

  /**
     If you wish to use the search bar as a controlled component, continuously pass the query via this prop.
     */
  query: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.string.isRequired]),

  /**
     Configures the search box. Set `placeholder` to change the placeholder text in the box and `incremental` to support incremental (as you type) search.
     */
  box: PropTypes.shape({
    name: PropTypes.string,
    id: PropTypes.string,
    placeholder: PropTypes.string,
    value: PropTypes.string,
    isInvalid: PropTypes.bool,
    fullWidth: PropTypes.bool,
    isLoading: PropTypes.bool,

    /**
       * Called when the user presses [Enter] OR on change if the incremental prop is `true`.
       * If you don't need the on[Enter] functionality, prefer using onChange
       */
    onSearch: PropTypes.func,

    /**
       * When `true` the search will be executed (that is, the `onSearch` will be called) as the
       * user types.
       */
    incremental: PropTypes.bool,

    /**
       * when `true` creates a shorter height input
       */
    compressed: PropTypes.bool,
    inputRef: PropTypes.func,

    /**
       * Shows a button that quickly clears any input
       */
    isClearable: PropTypes.bool,

    /**
       * Creates an input group with element(s) coming before input
       * `string` | `ReactElement` or an array of these
       */
    prepend: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

    /**
       * Creates an input group with element(s) coming after input.
       * `string` | `ReactElement` or an array of these
       */
    append: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,
    // Boolean values are not meaningful to this EuiSearchBox, but are allowed so that other
    // components can use e.g. a true value to mean "auto-derive a schema". See EuiInMemoryTable.
    // Admittedly, this is a bit of a hack.
    schema: PropTypes.oneOfType([PropTypes.shape({
      strict: PropTypes.bool,
      fields: PropTypes.any,
      flags: PropTypes.arrayOf(PropTypes.string.isRequired)
    }).isRequired, PropTypes.bool.isRequired])
  }),

  /**
     An array of search filters. See #SearchFilterConfig.
     */
  filters: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.shape({
    type: PropTypes.oneOf(["is"]).isRequired,
    field: PropTypes.string.isRequired,
    name: PropTypes.string.isRequired,
    negatedName: PropTypes.string,
    available: PropTypes.func
  }).isRequired, PropTypes.shape({
    type: PropTypes.oneOf(["field_value_selection"]).isRequired,
    field: PropTypes.string,
    name: PropTypes.string.isRequired,

    /**
       * See #FieldValueOptionType
       */
    options: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.shape({
      field: PropTypes.string,
      value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired, PropTypes.shape({
        type: PropTypes.oneOf(["date"]).isRequired,
        raw: PropTypes.any.isRequired,
        granularity: PropTypes.oneOfType([PropTypes.shape({
          es: PropTypes.oneOf(["d", "w", "M", "y"]).isRequired,
          js: PropTypes.oneOf(["day", "week", "month", "year"]).isRequired,
          isSame: PropTypes.func.isRequired,
          start: PropTypes.func.isRequired,
          startOfNext: PropTypes.func.isRequired,
          iso8601: PropTypes.func.isRequired
        }).isRequired, PropTypes.oneOf([undefined])]).isRequired,
        text: PropTypes.string.isRequired,
        resolve: PropTypes.func.isRequired
      }).isRequired]).isRequired,
      name: PropTypes.string,
      view: PropTypes.node
    }).isRequired).isRequired, PropTypes.func.isRequired]).isRequired,
    filterWith: PropTypes.oneOfType([PropTypes.oneOf(["prefix", "includes"]), PropTypes.func.isRequired]),
    cache: PropTypes.number,
    multiSelect: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.oneOf(["and", "or"])]),
    loadingMessage: PropTypes.string,
    noOptionsMessage: PropTypes.string,
    searchThreshold: PropTypes.number,
    available: PropTypes.func,
    autoClose: PropTypes.bool,
    operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
  }).isRequired, PropTypes.shape({
    type: PropTypes.oneOf(["field_value_toggle"]).isRequired,
    field: PropTypes.string.isRequired,
    value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired, PropTypes.shape({
      type: PropTypes.oneOf(["date"]).isRequired,
      raw: PropTypes.any.isRequired,
      granularity: PropTypes.oneOfType([PropTypes.shape({
        es: PropTypes.oneOf(["d", "w", "M", "y"]).isRequired,
        js: PropTypes.oneOf(["day", "week", "month", "year"]).isRequired,
        isSame: PropTypes.func.isRequired,
        start: PropTypes.func.isRequired,
        startOfNext: PropTypes.func.isRequired,
        iso8601: PropTypes.func.isRequired
      }).isRequired, PropTypes.oneOf([undefined])]).isRequired,
      text: PropTypes.string.isRequired,
      resolve: PropTypes.func.isRequired
    }).isRequired]).isRequired,
    name: PropTypes.string.isRequired,
    negatedName: PropTypes.string,
    available: PropTypes.func,
    operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
  }).isRequired, PropTypes.shape({
    type: PropTypes.oneOf(["field_value_toggle_group"]).isRequired,
    field: PropTypes.string.isRequired,

    /**
       * See #FieldValueToggleGroupFilterItemType
       */
    items: PropTypes.arrayOf(PropTypes.shape({
      value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired]).isRequired,
      name: PropTypes.string.isRequired,
      negatedName: PropTypes.string,
      operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
    }).isRequired).isRequired,
    available: PropTypes.func
  }).isRequired]).isRequired),

  /**
     * Tools which go to the left of the search bar.
     */
  toolsLeft: PropTypes.oneOfType([PropTypes.element.isRequired, PropTypes.arrayOf(PropTypes.element.isRequired).isRequired]),

  /**
     * Tools which go to the right of the search bar.
     */
  toolsRight: PropTypes.oneOfType([PropTypes.element.isRequired, PropTypes.arrayOf(PropTypes.element.isRequired).isRequired]),

  /**
     * Date formatter to use when parsing date values
     */
  dateFormat: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};