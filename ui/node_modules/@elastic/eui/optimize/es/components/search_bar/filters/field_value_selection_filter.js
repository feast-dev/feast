import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import { isArray, isNil } from '../../../services/predicate';
import { keys } from '../../../services';
import { EuiPopover, EuiPopoverTitle } from '../../popover';
import { EuiFieldSearch } from '../../form/field_search';
import { EuiFilterButton, EuiFilterSelectItem } from '../../filter_group';
import { EuiLoadingChart } from '../../loading';
import { EuiSpacer } from '../../spacer';
import { EuiIcon } from '../../icon';
import { Query } from '../query';
import { Operator } from '../query/ast';
import { jsx as ___EmotionJSX } from "@emotion/react";
var defaults = {
  config: {
    multiSelect: true,
    filterWith: 'prefix',
    loadingMessage: 'Loading...',
    noOptionsMessage: 'No options found',
    searchThreshold: 10
  }
};
export var FieldValueSelectionFilter = /*#__PURE__*/function (_Component) {
  _inherits(FieldValueSelectionFilter, _Component);

  var _super = _createSuper(FieldValueSelectionFilter);

  function FieldValueSelectionFilter(props) {
    var _this;

    _classCallCheck(this, FieldValueSelectionFilter);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "selectItems", void 0);

    _defineProperty(_assertThisInitialized(_this), "searchInput", null);

    _defineProperty(_assertThisInitialized(_this), "resolveOptionsLoader", function () {
      var options = _this.props.config.options;

      if (isArray(options)) {
        return function () {
          return Promise.resolve(options);
        };
      }

      return function () {
        var cachedOptions = _this.state.cachedOptions;

        if (cachedOptions) {
          return Promise.resolve(cachedOptions);
        }

        return options().then(function (opts) {
          // If a cache time is set, populate the cache and also schedule a
          // cache reset.
          if (_this.props.config.cache != null && _this.props.config.cache > 0) {
            _this.setState({
              cachedOptions: opts
            });

            setTimeout(function () {
              _this.setState({
                cachedOptions: null
              });
            }, _this.props.config.cache);
          }

          return opts;
        });
      };
    });

    var _options = props.config.options;
    var preloadedOptions = isArray(_options) ? {
      all: _options,
      shown: _options
    } : null;
    _this.selectItems = [];
    _this.state = {
      popoverOpen: false,
      error: null,
      options: preloadedOptions,
      activeItems: []
    };
    return _this;
  }

  _createClass(FieldValueSelectionFilter, [{
    key: "closePopover",
    value: function closePopover() {
      this.setState({
        popoverOpen: false
      });
    }
  }, {
    key: "onButtonClick",
    value: function onButtonClick() {
      var _this2 = this;

      this.setState(function (prevState) {
        if (!prevState.popoverOpen) {
          // loading options updates the state, so we'll do that in the animation frame
          window.requestAnimationFrame(function () {
            _this2.loadOptions();
          });
        }

        return {
          options: null,
          error: null,
          popoverOpen: !prevState.popoverOpen
        };
      });
    }
  }, {
    key: "loadOptions",
    value: function loadOptions() {
      var _this3 = this;

      var loader = this.resolveOptionsLoader();
      this.setState({
        options: null,
        error: null
      });
      loader().then(function (options) {
        var items = {
          on: [],
          off: [],
          rest: []
        };
        var _this3$props = _this3.props,
            query = _this3$props.query,
            config = _this3$props.config;

        var multiSelect = _this3.resolveMultiSelect();

        if (options) {
          options.forEach(function (op) {
            var optionField = op.field || config.field;

            if (optionField) {
              var clause = multiSelect === 'or' ? query.getOrFieldClause(optionField, op.value) : query.getSimpleFieldClause(optionField, op.value);

              var checked = _this3.resolveChecked(clause);

              if (!checked) {
                items.rest.push(op);
              } else if (checked === 'on') {
                items.on.push(op);
              } else {
                items.off.push(op);
              }
            }

            return;
          });
        }

        _this3.setState({
          error: null,
          activeItems: items.on,
          options: {
            all: options,
            shown: [].concat(_toConsumableArray(items.on), _toConsumableArray(items.off), _toConsumableArray(items.rest))
          }
        });
      }).catch(function () {
        _this3.setState({
          options: null,
          error: 'Could not load options'
        });
      });
    }
  }, {
    key: "filterOptions",
    value: function filterOptions() {
      var _this4 = this;

      var q = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : '';
      this.setState(function (prevState) {
        if (isNil(prevState.options)) {
          return {};
        }

        var predicate = _this4.getOptionFilter();

        return _objectSpread(_objectSpread({}, prevState), {}, {
          options: _objectSpread(_objectSpread({}, prevState.options), {}, {
            shown: prevState.options.all.filter(function (option, i, options) {
              var name = _this4.resolveOptionName(option).toLowerCase();

              var query = q.toLowerCase();
              return predicate(name, query, options);
            })
          })
        });
      });
    }
  }, {
    key: "getOptionFilter",
    value: function getOptionFilter() {
      var filterWith = this.props.config.filterWith || defaults.config.filterWith;

      if (typeof filterWith === 'function') {
        return filterWith;
      }

      if (filterWith === 'includes') {
        return function (name, query) {
          return name.includes(query);
        };
      }

      return function (name, query) {
        return name.startsWith(query);
      };
    }
  }, {
    key: "resolveOptionName",
    value: function resolveOptionName(option) {
      return option.name || option.value.toString();
    }
  }, {
    key: "onOptionClick",
    value: function onOptionClick(field, value, checked) {
      var multiSelect = this.resolveMultiSelect();
      var _this$props$config = this.props.config,
          _this$props$config$au = _this$props$config.autoClose,
          autoClose = _this$props$config$au === void 0 ? true : _this$props$config$au,
          _this$props$config$op = _this$props$config.operator,
          operator = _this$props$config$op === void 0 ? Operator.EQ : _this$props$config$op; // we're closing popover only if the user can only select one item... if the
      // user can select more, we'll leave it open so she can continue selecting

      if (!multiSelect && autoClose) {
        this.closePopover();

        var _query = checked ? this.props.query.removeSimpleFieldClauses(field) : this.props.query.removeSimpleFieldClauses(field).addSimpleFieldValue(field, value, true, operator);

        this.props.onChange(_query);
      } else {
        if (multiSelect === 'or') {
          var _query2 = checked ? this.props.query.removeOrFieldValue(field, value) : this.props.query.addOrFieldValue(field, value, true, operator);

          this.props.onChange(_query2);
        } else {
          var _query3 = checked ? this.props.query.removeSimpleFieldValue(field, value) : this.props.query.addSimpleFieldValue(field, value, true, operator);

          this.props.onChange(_query3);
        }
      }
    }
  }, {
    key: "onKeyDown",
    value: function onKeyDown(index, event) {
      switch (event.key) {
        case keys.ARROW_DOWN:
          if (index < this.selectItems.length - 1) {
            event.preventDefault();
            this.selectItems[index + 1].focus();
          }

          break;

        case keys.ARROW_UP:
          if (index < 0) {
            return; // it's coming from the search box... nothing to do... nowhere to go
          }

          if (index === 0 && this.searchInput) {
            event.preventDefault();
            this.searchInput.focus();
          } else if (index > 0) {
            event.preventDefault();
            this.selectItems[index - 1].focus();
          }

      }
    }
  }, {
    key: "resolveMultiSelect",
    value: function resolveMultiSelect() {
      var config = this.props.config;
      return !isNil(config.multiSelect) ? config.multiSelect : defaults.config.multiSelect;
    }
  }, {
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.props.query.text.length) this.loadOptions();
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (this.props.query !== prevProps.query) this.loadOptions();
    }
  }, {
    key: "render",
    value: function render() {
      var _this5 = this;

      var _this$props = this.props,
          query = _this$props.query,
          config = _this$props.config;
      var multiSelect = this.resolveMultiSelect();
      var activeTop = this.isActiveField(config.field);
      var activeItem = this.state.options ? this.state.options.all.some(function (item) {
        return _this5.isActiveField(item.field);
      }) : false;
      var activeItemsCount = this.state.activeItems.length;
      var active = (activeTop || activeItem) && activeItemsCount > 0;

      var button = ___EmotionJSX(EuiFilterButton, {
        iconType: "arrowDown",
        iconSide: "right",
        onClick: this.onButtonClick.bind(this),
        hasActiveFilters: active,
        numActiveFilters: active ? activeItemsCount : undefined,
        grow: true
      }, config.name);

      var searchBox = this.renderSearchBox();
      var content = this.renderContent(config.field, query, config, multiSelect);
      return ___EmotionJSX(EuiPopover, {
        button: button,
        isOpen: this.state.popoverOpen,
        closePopover: this.closePopover.bind(this),
        panelPaddingSize: "none",
        anchorPosition: "downCenter",
        panelClassName: "euiFilterGroup__popoverPanel"
      }, searchBox, content);
    }
  }, {
    key: "renderSearchBox",
    value: function renderSearchBox() {
      var _this6 = this;

      var threshold = this.props.config.searchThreshold || defaults.config.searchThreshold;

      if (this.state.options && this.state.options.all.length >= threshold) {
        var disabled = this.state.error != null;
        return ___EmotionJSX(EuiPopoverTitle, {
          paddingSize: "s"
        }, ___EmotionJSX(EuiFieldSearch, {
          inputRef: function inputRef(ref) {
            return _this6.searchInput = ref;
          },
          disabled: disabled,
          incremental: true,
          onSearch: function onSearch(query) {
            return _this6.filterOptions(query);
          },
          onKeyDown: this.onKeyDown.bind(this, -1),
          compressed: true
        }));
      }
    }
  }, {
    key: "renderContent",
    value: function renderContent(field, query, config, multiSelect) {
      var _this7 = this;

      if (this.state.error) {
        return this.renderError(this.state.error);
      }

      if (isNil(this.state.options)) {
        return this.renderLoader();
      }

      if (this.state.options.shown.length === 0) {
        return this.renderNoOptions();
      }

      if (this.state.options == null) {
        return;
      }

      var items = [];
      this.state.options.shown.forEach(function (option, index) {
        var optionField = option.field || field;

        if (optionField == null) {
          throw new Error('option.field or field should be provided in <FieldValueSelectionFilter/>');
        }

        var clause = multiSelect === 'or' ? query.getOrFieldClause(optionField, option.value) : query.getSimpleFieldClause(optionField, option.value);

        var checked = _this7.resolveChecked(clause);

        var onClick = function onClick() {
          // clicking a checked item will uncheck it and effective remove the filter (value = undefined)
          _this7.onOptionClick(optionField, option.value, checked);
        };

        var item = ___EmotionJSX(EuiFilterSelectItem, {
          key: index,
          checked: checked,
          onClick: onClick,
          ref: function ref(_ref) {
            return _this7.selectItems[index] = _ref;
          },
          onKeyDown: _this7.onKeyDown.bind(_this7, index)
        }, option.view ? option.view : _this7.resolveOptionName(option));

        items.push(item);
      });
      return ___EmotionJSX("div", {
        className: "euiFilterSelect__items"
      }, items);
    }
  }, {
    key: "resolveChecked",
    value: function resolveChecked(clause) {
      if (clause) {
        return Query.isMust(clause) ? 'on' : 'off';
      }
    }
  }, {
    key: "renderLoader",
    value: function renderLoader() {
      var message = this.props.config.loadingMessage || defaults.config.loadingMessage;
      return ___EmotionJSX("div", {
        className: "euiFilterSelect__note"
      }, ___EmotionJSX("div", {
        className: "euiFilterSelect__noteContent"
      }, ___EmotionJSX(EuiLoadingChart, {
        size: "m"
      }), ___EmotionJSX(EuiSpacer, {
        size: "xs"
      }), ___EmotionJSX("p", null, message)));
    }
  }, {
    key: "renderError",
    value: function renderError(message) {
      return ___EmotionJSX("div", {
        className: "euiFilterSelect__note"
      }, ___EmotionJSX("div", {
        className: "euiFilterSelect__noteContent"
      }, ___EmotionJSX(EuiIcon, {
        size: "m",
        type: "faceSad",
        color: "danger"
      }), ___EmotionJSX(EuiSpacer, {
        size: "xs"
      }), ___EmotionJSX("p", null, message)));
    }
  }, {
    key: "renderNoOptions",
    value: function renderNoOptions() {
      var message = this.props.config.noOptionsMessage || defaults.config.noOptionsMessage;
      return ___EmotionJSX("div", {
        className: "euiFilterSelect__note"
      }, ___EmotionJSX("div", {
        className: "euiFilterSelect__noteContent"
      }, ___EmotionJSX(EuiIcon, {
        type: "minusInCircle"
      }), ___EmotionJSX(EuiSpacer, {
        size: "xs"
      }), ___EmotionJSX("p", null, message)));
    }
  }, {
    key: "isActiveField",
    value: function isActiveField(field) {
      if (typeof field !== 'string') {
        return false;
      }

      var query = this.props.query;
      var multiSelect = this.resolveMultiSelect();

      if (multiSelect === 'or') {
        return query.hasOrFieldClause(field);
      }

      return query.hasSimpleFieldClause(field);
    }
  }]);

  return FieldValueSelectionFilter;
}(Component);