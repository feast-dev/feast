import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _typeof from "@babel/runtime/helpers/typeof";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

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