import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
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
import React, { Component, createRef } from 'react';
import classNames from 'classnames';
import { EuiSelectableSearch } from './selectable_search';
import { EuiSelectableMessage } from './selectable_message';
import { EuiSelectableList } from './selectable_list';
import { EuiLoadingSpinner } from '../loading';
import { EuiSpacer } from '../spacer';
import { getMatchingOptions } from './matching_options';
import { keys, htmlIdGenerator } from '../../services';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSelectable = /*#__PURE__*/function (_Component) {
  _inherits(EuiSelectable, _Component);

  var _super = _createSuper(EuiSelectable);

  function EuiSelectable(props) {
    var _this;

    _classCallCheck(this, EuiSelectable);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "containerRef", /*#__PURE__*/createRef());

    _defineProperty(_assertThisInitialized(_this), "optionsListRef", /*#__PURE__*/createRef());

    _defineProperty(_assertThisInitialized(_this), "preventOnFocus", false);

    _defineProperty(_assertThisInitialized(_this), "rootId", htmlIdGenerator());

    _defineProperty(_assertThisInitialized(_this), "hasActiveOption", function () {
      return _this.state.activeOptionIndex != null;
    });

    _defineProperty(_assertThisInitialized(_this), "onMouseDown", function () {
      // Bypass onFocus when a click event originates from this.containerRef.
      // Prevents onFocus from scrolling away from a clicked option and negating the selection event.
      // https://github.com/elastic/eui/issues/4147
      _this.preventOnFocus = true;
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      if (_this.preventOnFocus) {
        _this.preventOnFocus = false;
        return;
      }

      if (!_this.state.visibleOptions.length || _this.state.activeOptionIndex) {
        return;
      }

      var firstSelected = _this.state.visibleOptions.findIndex(function (option) {
        return option.checked && !option.disabled && !option.isGroupLabel;
      });

      if (firstSelected > -1) {
        _this.setState({
          activeOptionIndex: firstSelected,
          isFocused: true
        });
      } else {
        _this.setState({
          activeOptionIndex: _this.state.visibleOptions.findIndex(function (option) {
            return !option.disabled && !option.isGroupLabel;
          }),
          isFocused: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onKeyDown", function (event) {
      var optionsList = _this.optionsListRef.current;

      switch (event.key) {
        case keys.ARROW_UP:
          event.preventDefault();
          event.stopPropagation();

          _this.incrementActiveOptionIndex(-1);

          break;

        case keys.ARROW_DOWN:
          event.preventDefault();
          event.stopPropagation();

          _this.incrementActiveOptionIndex(1);

          break;

        case keys.ENTER:
          event.preventDefault();
          event.stopPropagation();

          if (_this.state.activeOptionIndex != null && optionsList) {
            optionsList.onAddOrRemoveOption(_this.state.visibleOptions[_this.state.activeOptionIndex]);
          }

          break;

        default:
          _this.setState({
            activeOptionIndex: undefined
          }, _this.onFocus);

          break;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "incrementActiveOptionIndex", function (amount) {
      // If there are no options available, do nothing.
      if (!_this.state.visibleOptions.length) {
        return;
      }

      _this.setState(function (_ref) {
        var activeOptionIndex = _ref.activeOptionIndex,
            visibleOptions = _ref.visibleOptions;
        var nextActiveOptionIndex;

        if (activeOptionIndex == null) {
          // If this is the beginning of the user's keyboard navigation of the menu, then we'll focus
          // either the first or last item.
          nextActiveOptionIndex = amount < 0 ? visibleOptions.length - 1 : 0;
        } else {
          nextActiveOptionIndex = activeOptionIndex + amount;

          if (nextActiveOptionIndex < 0) {
            nextActiveOptionIndex = visibleOptions.length - 1;
          } else if (nextActiveOptionIndex === visibleOptions.length) {
            nextActiveOptionIndex = 0;
          }
        } // Group titles and disabled options are included in option list but are not selectable


        var direction = amount > 0 ? 1 : -1;

        while (visibleOptions[nextActiveOptionIndex].isGroupLabel || visibleOptions[nextActiveOptionIndex].disabled) {
          nextActiveOptionIndex = nextActiveOptionIndex + direction;

          if (nextActiveOptionIndex < 0) {
            nextActiveOptionIndex = visibleOptions.length - 1;
          } else if (nextActiveOptionIndex === visibleOptions.length) {
            nextActiveOptionIndex = 0;
          }
        }

        return {
          activeOptionIndex: nextActiveOptionIndex
        };
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onSearchChange", function (visibleOptions, searchValue) {
      _this.setState({
        visibleOptions: visibleOptions,
        searchValue: searchValue,
        activeOptionIndex: undefined
      }, function () {
        if (_this.state.isFocused) {
          _this.onFocus();
        }
      });

      if (_this.props.searchProps && _this.props.searchProps.onSearch) {
        _this.props.searchProps.onSearch(searchValue, visibleOptions);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onContainerBlur", function (e) {
      var _e$relatedTarget, _e$relatedTarget$firs;

      // Ignore blur events when moving from search to option to avoid activeOptionIndex conflicts
      if (((_e$relatedTarget = e.relatedTarget) === null || _e$relatedTarget === void 0 ? void 0 : (_e$relatedTarget$firs = _e$relatedTarget.firstChild) === null || _e$relatedTarget$firs === void 0 ? void 0 : _e$relatedTarget$firs.id) === _this.rootId('listbox')) {
        return;
      }

      _this.setState({
        activeOptionIndex: undefined,
        isFocused: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onOptionClick", function (options) {
      var _this$props = _this.props,
          isPreFiltered = _this$props.isPreFiltered,
          onChange = _this$props.onChange,
          searchProps = _this$props.searchProps;
      var searchValue = _this.state.searchValue;
      var visibleOptions = getMatchingOptions(options, searchValue, isPreFiltered);

      _this.setState({
        visibleOptions: visibleOptions
      });

      if (onChange) {
        onChange(options);
      }

      if (searchProps && searchProps.onChange) {
        searchProps.onChange(visibleOptions, searchValue);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "scrollToItem", function (index, align) {
      var _this$optionsListRef$, _this$optionsListRef$2;

      (_this$optionsListRef$ = _this.optionsListRef.current) === null || _this$optionsListRef$ === void 0 ? void 0 : (_this$optionsListRef$2 = _this$optionsListRef$.listRef) === null || _this$optionsListRef$2 === void 0 ? void 0 : _this$optionsListRef$2.scrollToItem(index, align);
    });

    var _options = props.options,
        singleSelection = props.singleSelection,
        _isPreFiltered = props.isPreFiltered;
    var initialSearchValue = '';

    var _visibleOptions = getMatchingOptions(_options, initialSearchValue, _isPreFiltered); // ensure that the currently selected single option is active if it is in the visibleOptions


    var selectedOptions = _options.filter(function (option) {
      return option.checked;
    });

    var _activeOptionIndex;

    if (singleSelection && selectedOptions.length === 1) {
      if (_visibleOptions.includes(selectedOptions[0])) {
        _activeOptionIndex = _visibleOptions.indexOf(selectedOptions[0]);
      }
    }

    _this.state = {
      activeOptionIndex: _activeOptionIndex,
      searchValue: initialSearchValue,
      visibleOptions: _visibleOptions,
      isFocused: false
    };
    return _this;
  }

  _createClass(EuiSelectable, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props2 = this.props,
          id = _this$props2.id,
          children = _this$props2.children,
          className = _this$props2.className,
          options = _this$props2.options,
          onChange = _this$props2.onChange,
          searchable = _this$props2.searchable,
          searchProps = _this$props2.searchProps,
          singleSelection = _this$props2.singleSelection,
          isLoading = _this$props2.isLoading,
          listProps = _this$props2.listProps,
          renderOption = _this$props2.renderOption,
          height = _this$props2.height,
          allowExclusions = _this$props2.allowExclusions,
          ariaLabel = _this$props2['aria-label'],
          ariaDescribedby = _this$props2['aria-describedby'],
          loadingMessage = _this$props2.loadingMessage,
          noMatchesMessage = _this$props2.noMatchesMessage,
          emptyMessage = _this$props2.emptyMessage,
          isPreFiltered = _this$props2.isPreFiltered,
          rest = _objectWithoutProperties(_this$props2, ["id", "children", "className", "options", "onChange", "searchable", "searchProps", "singleSelection", "isLoading", "listProps", "renderOption", "height", "allowExclusions", "aria-label", "aria-describedby", "loadingMessage", "noMatchesMessage", "emptyMessage", "isPreFiltered"]);

      var _this$state = this.state,
          searchValue = _this$state.searchValue,
          visibleOptions = _this$state.visibleOptions,
          activeOptionIndex = _this$state.activeOptionIndex; // Some messy destructuring here to remove aria-label/describedby from searchProps and listProps
      // Made messier by some TS requirements
      // The aria attributes are then used in getAccessibleName() to place them where they need to go

      var unknownAccessibleName = {
        'aria-label': undefined,
        'aria-describedby': undefined
      };

      var _ref2 = searchProps || unknownAccessibleName,
          searchAriaLabel = _ref2['aria-label'],
          searchAriaDescribedby = _ref2['aria-describedby'],
          propsOnChange = _ref2.onChange,
          onSearch = _ref2.onSearch,
          cleanedSearchProps = _objectWithoutProperties(_ref2, ["aria-label", "aria-describedby", "onChange", "onSearch"]);

      var _ref3 = listProps || unknownAccessibleName,
          listAriaLabel = _ref3['aria-label'],
          listAriaDescribedby = _ref3['aria-describedby'],
          isVirtualized = _ref3.isVirtualized,
          rowHeight = _ref3.rowHeight,
          cleanedListProps = _objectWithoutProperties(_ref3, ["aria-label", "aria-describedby", "isVirtualized", "rowHeight"]);

      var virtualizedProps;

      if (isVirtualized === false) {
        virtualizedProps = {
          isVirtualized: isVirtualized
        };
      } else if (rowHeight != null) {
        virtualizedProps = {
          rowHeight: rowHeight
        };
      }

      var classes = classNames('euiSelectable', {
        'euiSelectable-fullHeight': height === 'full'
      }, className);
      /** Create Id's */

      var messageContentId = this.rootId('messageContent');
      var listId = this.rootId('listbox');

      var makeOptionId = function makeOptionId(index) {
        if (typeof index === 'undefined') {
          return '';
        }

        return "".concat(listId, "_option-").concat(index);
      };
      /** Create message content that replaces the list if no options are available (yet) */


      var messageContent;

      if (isLoading) {
        if (loadingMessage === undefined || typeof loadingMessage === 'string') {
          messageContent = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiLoadingSpinner, {
            size: "m"
          }), ___EmotionJSX(EuiSpacer, {
            size: "xs"
          }), ___EmotionJSX("p", null, loadingMessage || ___EmotionJSX(EuiI18n, {
            token: "euiSelectable.loadingOptions",
            default: "Loading options"
          })));
        } else {
          messageContent = /*#__PURE__*/React.cloneElement(loadingMessage, _objectSpread({
            id: messageContentId
          }, loadingMessage.props));
        }
      } else if (searchValue && visibleOptions.length === 0) {
        if (noMatchesMessage === undefined || typeof noMatchesMessage === 'string') {
          messageContent = ___EmotionJSX("p", null, noMatchesMessage || ___EmotionJSX(EuiI18n, {
            token: "euiSelectable.noMatchingOptions",
            default: "{searchValue} doesn't match any options",
            values: {
              searchValue: ___EmotionJSX("strong", null, searchValue)
            }
          }));
        } else {
          messageContent = /*#__PURE__*/React.cloneElement(noMatchesMessage, _objectSpread({
            id: messageContentId
          }, noMatchesMessage.props));
        }
      } else if (!options.length) {
        if (emptyMessage === undefined || typeof emptyMessage === 'string') {
          messageContent = ___EmotionJSX("p", null, emptyMessage || ___EmotionJSX(EuiI18n, {
            token: "euiSelectable.noAvailableOptions",
            default: "No options available"
          }));
        } else {
          messageContent = /*#__PURE__*/React.cloneElement(emptyMessage, _objectSpread({
            id: messageContentId
          }, emptyMessage.props));
        }
      } else {
        messageContentId = '';
      }
      /**
       * There are lots of ways to add an accessible name
       * Usually we want the same name for the input and the listbox (which is added by aria-label/describedby)
       * But you can always override it using searchProps or listProps
       * This finds the correct name to use
       *
       * TODO: This doesn't handle being labelled (<label for="idOfInput">)
       */


      var getAccessibleName = function getAccessibleName(props, messageContentId) {
        if (props && props['aria-label']) {
          return {
            'aria-label': props['aria-label']
          };
        }

        var messageContentIdString = messageContentId ? " ".concat(messageContentId) : '';

        if (props && props['aria-describedby']) {
          return {
            'aria-describedby': "".concat(props['aria-describedby']).concat(messageContentIdString)
          };
        }

        if (ariaLabel) {
          return {
            'aria-label': ariaLabel
          };
        }

        if (ariaDescribedby) {
          return {
            'aria-describedby': "".concat(ariaDescribedby).concat(messageContentIdString)
          };
        }

        return {};
      };

      var searchAccessibleName = getAccessibleName(searchProps, messageContentId);
      var searchHasAccessibleName = Boolean(Object.keys(searchAccessibleName).length);
      var search = searchable ? ___EmotionJSX(EuiI18n, {
        token: "euiSelectable.placeholderName",
        default: "Filter options"
      }, function (placeholderName) {
        return ___EmotionJSX(EuiSelectableSearch, _extends({
          key: "listSearch",
          options: options,
          onChange: _this2.onSearchChange,
          listId: _this2.optionsListRef.current ? listId : undefined // Only pass the listId if it exists on the page
          ,
          "aria-activedescendant": makeOptionId(activeOptionIndex) // the current faux-focused option
          ,
          placeholder: placeholderName,
          isPreFiltered: isPreFiltered !== null && isPreFiltered !== void 0 ? isPreFiltered : false
        }, searchHasAccessibleName ? searchAccessibleName : {
          'aria-label': placeholderName
        }, cleanedSearchProps));
      }) : undefined;
      var listAccessibleName = getAccessibleName(listProps);
      var listHasAccessibleName = Boolean(Object.keys(listAccessibleName).length);
      var list = messageContent ? ___EmotionJSX(EuiSelectableMessage, {
        id: messageContentId,
        bordered: listProps && listProps.bordered
      }, messageContent) : ___EmotionJSX(EuiI18n, {
        token: "euiSelectable.placeholderName",
        default: "Filter options"
      }, function (placeholderName) {
        return ___EmotionJSX(EuiSelectableList, _extends({
          key: "list",
          options: options,
          visibleOptions: visibleOptions,
          searchValue: searchValue,
          activeOptionIndex: activeOptionIndex,
          setActiveOptionIndex: function setActiveOptionIndex(index, cb) {
            _this2.setState({
              activeOptionIndex: index
            }, cb);
          },
          onOptionClick: _this2.onOptionClick,
          singleSelection: singleSelection,
          ref: _this2.optionsListRef,
          renderOption: renderOption,
          height: height,
          allowExclusions: allowExclusions,
          searchable: searchable,
          makeOptionId: makeOptionId,
          listId: listId
        }, listHasAccessibleName ? listAccessibleName : searchable && {
          'aria-label': placeholderName
        }, cleanedListProps, virtualizedProps));
      });
      return ___EmotionJSX("div", _extends({
        ref: this.containerRef,
        className: classes,
        onKeyDown: this.onKeyDown,
        onBlur: this.onContainerBlur,
        onFocus: this.onFocus,
        onMouseDown: this.onMouseDown
      }, rest), children && children(list, search));
    }
  }], [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      var options = nextProps.options,
          isPreFiltered = nextProps.isPreFiltered;
      var activeOptionIndex = prevState.activeOptionIndex,
          searchValue = prevState.searchValue;
      var matchingOptions = getMatchingOptions(options, searchValue, isPreFiltered);
      var stateUpdate = {
        visibleOptions: matchingOptions,
        activeOptionIndex: activeOptionIndex
      };

      if (activeOptionIndex != null && activeOptionIndex >= matchingOptions.length) {
        stateUpdate.activeOptionIndex = -1;
      }

      return stateUpdate;
    }
  }]);

  return EuiSelectable;
}(Component);

_defineProperty(EuiSelectable, "defaultProps", {
  options: [],
  singleSelection: false,
  searchable: false,
  isPreFiltered: false
});