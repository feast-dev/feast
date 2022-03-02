import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _createClass from "@babel/runtime/helpers/createClass";
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
import React, { Component, memo } from 'react';
import classNames from 'classnames';
import { FixedSizeList, areEqual } from 'react-window';
import { EuiAutoSizer } from '../../auto_sizer';
import { EuiHighlight } from '../../highlight';
import { EuiSelectableListItem } from './selectable_list_item';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSelectableList = /*#__PURE__*/function (_Component) {
  _inherits(EuiSelectableList, _Component);

  var _super = _createSuper(EuiSelectableList);

  _createClass(EuiSelectableList, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate() {
      var activeOptionIndex = this.props.activeOptionIndex;

      if (this.listBoxRef && this.props.searchable !== true) {
        this.listBoxRef.setAttribute('aria-activedescendant', "".concat(this.props.makeOptionId(activeOptionIndex)));
      }

      if (this.listRef && typeof this.props.activeOptionIndex !== 'undefined') {
        this.listRef.scrollToItem(this.props.activeOptionIndex, 'auto');
      }
    }
  }]);

  function EuiSelectableList(props) {
    var _this;

    _classCallCheck(this, EuiSelectableList);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "listRef", null);

    _defineProperty(_assertThisInitialized(_this), "listBoxRef", null);

    _defineProperty(_assertThisInitialized(_this), "setListRef", function (ref) {
      _this.listRef = ref;

      if (ref && _this.props.activeOptionIndex) {
        ref.scrollToItem(_this.props.activeOptionIndex, 'auto');
      }
    });

    _defineProperty(_assertThisInitialized(_this), "removeScrollableTabStop", function (ref) {
      // Firefox adds a tab stop for scrollable containers
      // We handle this inside so need to stop firefox from doing its thing
      if (ref) {
        ref.setAttribute('tabindex', '-1');
      }
    });

    _defineProperty(_assertThisInitialized(_this), "setListBoxRef", function (ref) {
      _this.listBoxRef = ref;
      var _this$props = _this.props,
          listId = _this$props.listId,
          searchable = _this$props.searchable,
          singleSelection = _this$props.singleSelection,
          ariaLabel = _this$props['aria-label'],
          ariaLabelledby = _this$props['aria-labelledby'],
          ariaDescribedby = _this$props['aria-describedby'];

      if (ref) {
        ref.setAttribute('id', listId);
        ref.setAttribute('role', 'listbox');

        if (searchable !== true) {
          ref.setAttribute('tabindex', '0');

          if (singleSelection !== 'always' && singleSelection !== true) {
            ref.setAttribute('aria-multiselectable', 'true');
          }
        }

        if (typeof ariaLabel === 'string') {
          ref.setAttribute('aria-label', ariaLabel);
        } else if (typeof ariaLabelledby === 'string') {
          ref.setAttribute('aria-labelledby', ariaLabelledby);
        }

        if (typeof ariaDescribedby === 'string') {
          ref.setAttribute('aria-labelledby', ariaDescribedby);
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "ListRow", /*#__PURE__*/memo(function (_ref) {
      var data = _ref.data,
          index = _ref.index,
          style = _ref.style;
      var option = data[index];

      var optionData = option.data,
          _option = _objectWithoutProperties(option, ["data"]);

      var label = option.label,
          isGroupLabel = option.isGroupLabel,
          checked = option.checked,
          disabled = option.disabled,
          prepend = option.prepend,
          append = option.append,
          ref = option.ref,
          key = option.key,
          searchableLabel = option.searchableLabel,
          _data = option.data,
          optionRest = _objectWithoutProperties(option, ["label", "isGroupLabel", "checked", "disabled", "prepend", "append", "ref", "key", "searchableLabel", "data"]);

      if (isGroupLabel) {
        return ___EmotionJSX("li", _extends({
          role: "presentation",
          className: "euiSelectableList__groupLabel",
          style: style // @ts-ignore complex

        }, optionRest), prepend, label, append);
      }

      var labelCount = data.filter(function (option) {
        return option.isGroupLabel;
      }).length;
      return ___EmotionJSX(EuiSelectableListItem, _extends({
        id: _this.props.makeOptionId(index),
        style: style,
        key: key || label.toLowerCase(),
        onMouseDown: function onMouseDown() {
          _this.props.setActiveOptionIndex(index);
        },
        onClick: function onClick() {
          return _this.onAddOrRemoveOption(option);
        },
        ref: ref ? ref.bind(null, index) : undefined,
        isFocused: _this.props.activeOptionIndex === index,
        title: searchableLabel || label,
        checked: checked,
        disabled: disabled,
        prepend: prepend,
        append: append,
        "aria-posinset": index + 1 - labelCount,
        "aria-setsize": data.length - labelCount,
        onFocusBadge: _this.props.onFocusBadge,
        allowExclusions: _this.props.allowExclusions,
        showIcons: _this.props.showIcons
      }, optionRest), _this.props.renderOption ? _this.props.renderOption( // @ts-ignore complex
      _objectSpread(_objectSpread({}, _option), optionData), _this.props.searchValue) : ___EmotionJSX(EuiHighlight, {
        search: _this.props.searchValue
      }, label));
    }, areEqual));

    _defineProperty(_assertThisInitialized(_this), "onAddOrRemoveOption", function (option) {
      if (option.disabled) {
        return;
      }

      var _this$props2 = _this.props,
          allowExclusions = _this$props2.allowExclusions,
          options = _this$props2.options,
          _this$props2$visibleO = _this$props2.visibleOptions,
          visibleOptions = _this$props2$visibleO === void 0 ? options : _this$props2$visibleO;

      _this.props.setActiveOptionIndex(visibleOptions.findIndex(function (_ref2) {
        var label = _ref2.label;
        return label === option.label;
      }), function () {
        if (option.checked === 'on' && allowExclusions) {
          _this.onExcludeOption(option);
        } else if (option.checked === 'on' || option.checked === 'off') {
          _this.onRemoveOption(option);
        } else {
          _this.onAddOption(option);
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onAddOption", function (addedOption) {
      var _this$props3 = _this.props,
          onOptionClick = _this$props3.onOptionClick,
          options = _this$props3.options,
          singleSelection = _this$props3.singleSelection;
      var updatedOptions = options.map(function (option) {
        // if singleSelection is enabled, uncheck any selected option(s)
        var updatedOption = _objectSpread({}, option);

        if (singleSelection) {
          delete updatedOption.checked;
        } // if this is the now-selected option, check it


        if (option === addedOption) {
          updatedOption.checked = 'on';
        }

        return updatedOption;
      });
      onOptionClick(updatedOptions);
    });

    _defineProperty(_assertThisInitialized(_this), "onRemoveOption", function (removedOption) {
      var _this$props4 = _this.props,
          onOptionClick = _this$props4.onOptionClick,
          singleSelection = _this$props4.singleSelection,
          options = _this$props4.options;
      var updatedOptions = options.map(function (option) {
        var updatedOption = _objectSpread({}, option);

        if (option === removedOption && singleSelection !== 'always') {
          delete updatedOption.checked;
        }

        return updatedOption;
      });
      onOptionClick(updatedOptions);
    });

    _defineProperty(_assertThisInitialized(_this), "onExcludeOption", function (excludedOption) {
      var _this$props5 = _this.props,
          onOptionClick = _this$props5.onOptionClick,
          options = _this$props5.options;
      excludedOption.checked = 'off';
      var updatedOptions = options.map(function (option) {
        var updatedOption = _objectSpread({}, option);

        if (option === excludedOption) {
          updatedOption.checked = 'off';
        }

        return updatedOption;
      });
      onOptionClick(updatedOptions);
    });

    return _this;
  }

  _createClass(EuiSelectableList, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props6 = this.props,
          className = _this$props6.className,
          options = _this$props6.options,
          searchValue = _this$props6.searchValue,
          onOptionClick = _this$props6.onOptionClick,
          renderOption = _this$props6.renderOption,
          forcedHeight = _this$props6.height,
          windowProps = _this$props6.windowProps,
          rowHeight = _this$props6.rowHeight,
          activeOptionIndex = _this$props6.activeOptionIndex,
          makeOptionId = _this$props6.makeOptionId,
          showIcons = _this$props6.showIcons,
          singleSelection = _this$props6.singleSelection,
          visibleOptions = _this$props6.visibleOptions,
          allowExclusions = _this$props6.allowExclusions,
          bordered = _this$props6.bordered,
          searchable = _this$props6.searchable,
          onFocusBadge = _this$props6.onFocusBadge,
          listId = _this$props6.listId,
          setActiveOptionIndex = _this$props6.setActiveOptionIndex,
          ariaLabel = _this$props6['aria-label'],
          ariaLabelledby = _this$props6['aria-labelledby'],
          ariaDescribedby = _this$props6['aria-describedby'],
          isVirtualized = _this$props6.isVirtualized,
          rest = _objectWithoutProperties(_this$props6, ["className", "options", "searchValue", "onOptionClick", "renderOption", "height", "windowProps", "rowHeight", "activeOptionIndex", "makeOptionId", "showIcons", "singleSelection", "visibleOptions", "allowExclusions", "bordered", "searchable", "onFocusBadge", "listId", "setActiveOptionIndex", "aria-label", "aria-labelledby", "aria-describedby", "isVirtualized"]);

      var optionArray = visibleOptions || options;
      var heightIsFull = forcedHeight === 'full';
      var calculatedHeight = heightIsFull ? false : forcedHeight; // If calculatedHeight is still undefined, then calculate it

      if (calculatedHeight === undefined) {
        var maxVisibleOptions = 7;
        var numVisibleOptions = optionArray.length;
        var numVisibleMoreThanMax = optionArray.length > maxVisibleOptions;

        if (numVisibleMoreThanMax) {
          // Show only half of the last one to indicate there's more to scroll to
          calculatedHeight = (maxVisibleOptions - 0.5) * rowHeight;
        } else {
          calculatedHeight = numVisibleOptions * rowHeight;
        }
      }

      var classes = classNames('euiSelectableList', {
        'euiSelectableList-fullHeight': heightIsFull,
        'euiSelectableList-bordered': bordered
      }, className);
      return ___EmotionJSX("div", _extends({
        className: classes
      }, rest), isVirtualized ? ___EmotionJSX(EuiAutoSizer, {
        disableHeight: !heightIsFull
      }, function (_ref3) {
        var width = _ref3.width,
            height = _ref3.height;
        return ___EmotionJSX(FixedSizeList, _extends({
          ref: _this2.setListRef,
          outerRef: _this2.removeScrollableTabStop,
          className: "euiSelectableList__list",
          "data-skip-axe": "scrollable-region-focusable",
          width: width,
          height: calculatedHeight || height,
          itemCount: optionArray.length,
          itemData: optionArray,
          itemSize: rowHeight,
          innerElementType: "ul",
          innerRef: _this2.setListBoxRef
        }, windowProps), _this2.ListRow);
      }) : ___EmotionJSX("div", {
        className: "euiSelectableList__list",
        ref: this.removeScrollableTabStop
      }, ___EmotionJSX("ul", {
        ref: this.setListBoxRef
      }, optionArray.map(function (_, index) {
        return /*#__PURE__*/React.createElement(_this2.ListRow, {
          key: index,
          data: optionArray,
          index: index
        }, null);
      }))));
    }
  }]);

  return EuiSelectableList;
}(Component);

_defineProperty(EuiSelectableList, "defaultProps", {
  rowHeight: 32,
  searchValue: '',
  isVirtualized: true
});