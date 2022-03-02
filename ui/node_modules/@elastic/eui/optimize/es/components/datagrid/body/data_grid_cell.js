import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

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
import classNames from 'classnames';
import React, { Component, createRef, memo } from 'react';
import { createPortal } from 'react-dom';
import tabbable from 'tabbable';
import { keys } from '../../../services';
import { EuiScreenReaderOnly } from '../../accessibility';
import { EuiFocusTrap } from '../../focus_trap';
import { useEuiI18n } from '../../i18n';
import { hasResizeObserver } from '../../observer/resize_observer/resize_observer';
import { DataGridFocusContext } from '../utils/focus';
import { EuiDataGridCellButtons } from './data_grid_cell_buttons';
import { EuiDataGridCellPopover } from './data_grid_cell_popover';
import { IS_JEST_ENVIRONMENT } from '../../../test';
import { jsx as ___EmotionJSX } from "@emotion/react";
var EuiDataGridCellContent = /*#__PURE__*/memo(function (_ref) {
  var renderCellValue = _ref.renderCellValue,
      column = _ref.column,
      setCellContentsRef = _ref.setCellContentsRef,
      rowHeightsOptions = _ref.rowHeightsOptions,
      rowIndex = _ref.rowIndex,
      colIndex = _ref.colIndex,
      rowHeightUtils = _ref.rowHeightUtils,
      isDefinedHeight = _ref.isDefinedHeight,
      rest = _objectWithoutProperties(_ref, ["renderCellValue", "column", "setCellContentsRef", "rowHeightsOptions", "rowIndex", "colIndex", "rowHeightUtils", "isDefinedHeight"]);

  // React is more permissible than the TS types indicate
  var CellElement = renderCellValue;
  var positionText = useEuiI18n('euiDataGridCell.position', 'Row: {row}; Column: {col}', {
    row: rowIndex + 1,
    col: colIndex + 1
  });
  return ___EmotionJSX(React.Fragment, null, ___EmotionJSX("div", {
    ref: setCellContentsRef,
    "data-datagrid-cellcontent": true,
    className: isDefinedHeight ? 'euiDataGridRowCell__definedHeight' : 'euiDataGridRowCell__truncate',
    style: isDefinedHeight ? rowHeightUtils === null || rowHeightUtils === void 0 ? void 0 : rowHeightUtils.getStylesForCell(rowHeightsOptions, rowIndex) : {}
  }, ___EmotionJSX(CellElement, _extends({
    isDetails: false,
    "data-test-subj": "cell-content",
    rowIndex: rowIndex
  }, rest))), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", null, positionText)));
});
export var EuiDataGridCell = /*#__PURE__*/function (_Component) {
  _inherits(EuiDataGridCell, _Component);

  var _super = _createSuper(EuiDataGridCell);

  function EuiDataGridCell() {
    var _this;

    _classCallCheck(this, EuiDataGridCell);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "cellRef", /*#__PURE__*/createRef());

    _defineProperty(_assertThisInitialized(_this), "contentObserver", void 0);

    _defineProperty(_assertThisInitialized(_this), "popoverPanelRef", /*#__PURE__*/createRef());

    _defineProperty(_assertThisInitialized(_this), "cellContentsRef", null);

    _defineProperty(_assertThisInitialized(_this), "state", {
      cellProps: {},
      popoverIsOpen: false,
      isFocused: false,
      isEntered: false,
      enableInteractions: false,
      disableCellTabIndex: false
    });

    _defineProperty(_assertThisInitialized(_this), "unsubscribeCell", void 0);

    _defineProperty(_assertThisInitialized(_this), "focusTimeout", void 0);

    _defineProperty(_assertThisInitialized(_this), "style", null);

    _defineProperty(_assertThisInitialized(_this), "getInteractables", function () {
      var tabbingRef = _this.cellContentsRef;

      if (tabbingRef) {
        return tabbingRef.querySelectorAll('[data-datagrid-interactable=true]');
      }

      return [];
    });

    _defineProperty(_assertThisInitialized(_this), "takeFocus", function (preventScroll) {
      var cell = _this.cellRef.current;

      if (cell) {
        // only update focus if we are not already focused on something in this cell
        var element = document.activeElement;

        while (element != null && element !== cell) {
          element = element.parentElement;
        }

        var doFocusUpdate = element !== cell;

        if (doFocusUpdate) {
          var interactables = _this.getInteractables();

          if (_this.props.isExpandable === false && interactables.length === 1) {
            // Only one element can be interacted with
            interactables[0].focus({
              preventScroll: preventScroll
            });
          } else {
            cell.focus({
              preventScroll: preventScroll
            });
          }
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "recalculateAutoHeight", function () {
      var _this$props = _this.props,
          rowHeightUtils = _this$props.rowHeightUtils,
          rowHeightsOptions = _this$props.rowHeightsOptions,
          rowIndex = _this$props.rowIndex;

      if (_this.cellContentsRef && rowHeightUtils && rowHeightUtils.isAutoHeight(rowIndex, rowHeightsOptions)) {
        var _this$props2 = _this.props,
            columnId = _this$props2.columnId,
            visibleRowIndex = _this$props2.visibleRowIndex;
        var rowHeight = _this.cellContentsRef.offsetHeight;
        rowHeightUtils.setRowHeight(rowIndex, columnId, rowHeight, visibleRowIndex);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "recalculateLineHeight", function () {
      if (!_this.props.setRowHeight) return; // setRowHeight is only passed by data_grid_body into one cell per row

      if (!_this.cellContentsRef) return;
      var _this$props3 = _this.props,
          rowHeightUtils = _this$props3.rowHeightUtils,
          rowHeightsOptions = _this$props3.rowHeightsOptions,
          rowIndex = _this$props3.rowIndex;
      var rowHeightOption = rowHeightUtils === null || rowHeightUtils === void 0 ? void 0 : rowHeightUtils.getRowHeightOption(rowIndex, rowHeightsOptions);
      var isSingleLine = rowHeightOption == null; // Undefined rowHeightsOptions default to a single line

      var lineCount = isSingleLine ? 1 : rowHeightUtils === null || rowHeightUtils === void 0 ? void 0 : rowHeightUtils.getLineCount(rowHeightOption);

      if (lineCount) {
        var shouldUseHeightsCache = rowHeightUtils === null || rowHeightUtils === void 0 ? void 0 : rowHeightUtils.isRowHeightOverride(rowIndex, rowHeightsOptions);
        var height = rowHeightUtils.calculateHeightForLineCount(_this.cellContentsRef, lineCount, shouldUseHeightsCache);

        if (shouldUseHeightsCache) {
          var _this$props4 = _this.props,
              columnId = _this$props4.columnId,
              visibleRowIndex = _this$props4.visibleRowIndex;
          rowHeightUtils === null || rowHeightUtils === void 0 ? void 0 : rowHeightUtils.setRowHeight(rowIndex, columnId, height, visibleRowIndex);
        } else {
          _this.props.setRowHeight(height);
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "isFocusedCell", function () {
      var _this$context$focused, _this$context$focused2;

      return ((_this$context$focused = _this.context.focusedCell) === null || _this$context$focused === void 0 ? void 0 : _this$context$focused[0]) === _this.props.colIndex && ((_this$context$focused2 = _this.context.focusedCell) === null || _this$context$focused2 === void 0 ? void 0 : _this$context$focused2[1]) === _this.props.visibleRowIndex;
    });

    _defineProperty(_assertThisInitialized(_this), "onFocusUpdate", function (isFocused) {
      var preventScroll = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;

      _this.setState({
        isFocused: isFocused
      }, function () {
        if (isFocused) {
          _this.takeFocus(preventScroll);
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "setCellProps", function (cellProps) {
      _this.setState({
        cellProps: cellProps
      });
    });

    _defineProperty(_assertThisInitialized(_this), "setCellContentsRef", function (ref) {
      _this.cellContentsRef = ref;

      if (ref && hasResizeObserver) {
        _this.contentObserver = new window.ResizeObserver(function () {
          _this.recalculateAutoHeight();

          _this.recalculateLineHeight();
        });

        _this.contentObserver.observe(ref);
      } else if (_this.contentObserver) {
        _this.contentObserver.disconnect();
      }

      _this.preventTabbing();
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function (e) {
      // only perform this logic when the event's originating element (e.target) is
      // the wrapping element with the onFocus logic
      // reasons:
      //  * the outcome is only meaningful when the focus shifts to the wrapping element
      //  * if the cell children include portalled content React will bubble the focus
      //      event up, which can trigger the focus() call below, causing focus lock fighting
      if (_this.cellRef.current === e.target) {
        var _this$props5 = _this.props,
            colIndex = _this$props5.colIndex,
            visibleRowIndex = _this$props5.visibleRowIndex,
            isExpandable = _this$props5.isExpandable; // focus in next tick to give potential focus capturing mechanisms time to release their traps
        // also clear any previous focus timeout that may still be queued

        if (EuiDataGridCell.activeFocusTimeoutId) {
          window.clearTimeout(EuiDataGridCell.activeFocusTimeoutId);
        }

        EuiDataGridCell.activeFocusTimeoutId = _this.focusTimeout = window.setTimeout(function () {
          _this.context.setFocusedCell([colIndex, visibleRowIndex]);

          var interactables = _this.getInteractables();

          if (interactables.length === 1 && isExpandable === false) {
            interactables[0].focus();

            _this.setState({
              disableCellTabIndex: true
            });
          }
        }, 0);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onBlur", function () {
      _this.setState({
        disableCellTabIndex: false
      });
    });

    _defineProperty(_assertThisInitialized(_this), "preventTabbing", function () {
      if (_this.cellContentsRef) {
        var tabbables = tabbable(_this.cellContentsRef);

        for (var i = 0; i < tabbables.length; i++) {
          var element = tabbables[i];
          element.setAttribute('tabIndex', '-1');
          element.setAttribute('data-datagrid-interactable', 'true');
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "enableTabbing", function () {
      if (_this.cellContentsRef) {
        var interactables = _this.getInteractables();

        for (var i = 0; i < interactables.length; i++) {
          var element = interactables[i];
          element.removeAttribute('tabIndex');
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      _this.setState({
        popoverIsOpen: false
      });
    });

    return _this;
  }

  _createClass(EuiDataGridCell, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      var _this$props6 = this.props,
          colIndex = _this$props6.colIndex,
          visibleRowIndex = _this$props6.visibleRowIndex;
      this.unsubscribeCell = this.context.onFocusUpdate([colIndex, visibleRowIndex], this.onFocusUpdate); // Account for virtualization - when a cell unmounts when scrolled out of view
      // and then remounts when scrolled back into view, it should retain focus state

      if (this.isFocusedCell()) {
        // The second flag sets preventScroll: true as a focus option, which prevents
        // hijacking the user's scroll behavior when the cell re-mounts on scroll
        this.onFocusUpdate(true, true);
        this.context.setIsFocusedCellInView(true);
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      window.clearTimeout(this.focusTimeout);

      if (this.unsubscribeCell) {
        this.unsubscribeCell();
      }

      if (this.isFocusedCell()) {
        this.context.setIsFocusedCellInView(false);
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var _this$props$rowHeight, _prevProps$rowHeights;

      this.recalculateAutoHeight();

      if (((_this$props$rowHeight = this.props.rowHeightsOptions) === null || _this$props$rowHeight === void 0 ? void 0 : _this$props$rowHeight.defaultHeight) !== ((_prevProps$rowHeights = prevProps.rowHeightsOptions) === null || _prevProps$rowHeights === void 0 ? void 0 : _prevProps$rowHeights.defaultHeight)) {
        this.recalculateLineHeight();
      }

      if (this.props.columnId !== prevProps.columnId) {
        this.setCellProps({});
      }
    }
  }, {
    key: "shouldComponentUpdate",
    value: function shouldComponentUpdate(nextProps, nextState) {
      if (nextProps.rowIndex !== this.props.rowIndex) return true;
      if (nextProps.visibleRowIndex !== this.props.visibleRowIndex) return true;
      if (nextProps.colIndex !== this.props.colIndex) return true;
      if (nextProps.columnId !== this.props.columnId) return true;
      if (nextProps.columnType !== this.props.columnType) return true;
      if (nextProps.width !== this.props.width) return true;
      if (nextProps.rowHeightsOptions !== this.props.rowHeightsOptions) return true;
      if (nextProps.renderCellValue !== this.props.renderCellValue) return true;
      if (nextProps.interactiveCellId !== this.props.interactiveCellId) return true;
      if (nextProps.popoverContent !== this.props.popoverContent) return true; // respond to adjusted position & dimensions

      if (nextProps.style) {
        if (!this.props.style) return true;

        if (nextProps.style.top !== this.props.style.top) {
          return true;
        }

        if (nextProps.style.left !== this.props.style.left) return true;
        if (nextProps.style.height !== this.props.style.height) return true;
        if (nextProps.style.width !== this.props.style.width) return true;
      }

      if (nextState.cellProps !== this.state.cellProps) return true;
      if (nextState.popoverIsOpen !== this.state.popoverIsOpen) return true;
      if (nextState.isEntered !== this.state.isEntered) return true;
      if (nextState.isFocused !== this.state.isFocused) return true;
      if (nextState.enableInteractions !== this.state.enableInteractions) return true;
      if (nextState.disableCellTabIndex !== this.state.disableCellTabIndex) return true;
      return false;
    }
  }, {
    key: "render",
    value: function render() {
      var _classNames,
          _rowHeightsOptions$li,
          _this2 = this;

      var _this$props7 = this.props,
          width = _this$props7.width,
          isExpandable = _this$props7.isExpandable,
          PopoverContent = _this$props7.popoverContent,
          interactiveCellId = _this$props7.interactiveCellId,
          columnType = _this$props7.columnType,
          className = _this$props7.className,
          column = _this$props7.column,
          style = _this$props7.style,
          rowHeightUtils = _this$props7.rowHeightUtils,
          rowHeightsOptions = _this$props7.rowHeightsOptions,
          rowManager = _this$props7.rowManager,
          rest = _objectWithoutProperties(_this$props7, ["width", "isExpandable", "popoverContent", "interactiveCellId", "columnType", "className", "column", "style", "rowHeightUtils", "rowHeightsOptions", "rowManager"]);

      var rowIndex = rest.rowIndex;
      var showCellButtons = this.state.isFocused || this.state.isEntered || this.state.enableInteractions || this.state.popoverIsOpen;
      var cellClasses = classNames('euiDataGridRowCell', (_classNames = {}, _defineProperty(_classNames, "euiDataGridRowCell--".concat(columnType), columnType), _defineProperty(_classNames, 'euiDataGridRowCell--open', this.state.popoverIsOpen), _classNames), className);

      var cellProps = _objectSpread(_objectSpread({}, this.state.cellProps), {}, {
        'data-test-subj': classNames('dataGridRowCell', this.state.cellProps['data-test-subj']),
        className: classNames(cellClasses, this.state.cellProps.className)
      });

      cellProps.style = _objectSpread(_objectSpread({}, style), {}, {
        // from react-window
        width: width,
        // column width, can be undefined
        lineHeight: (_rowHeightsOptions$li = rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : rowHeightsOptions.lineHeight) !== null && _rowHeightsOptions$li !== void 0 ? _rowHeightsOptions$li : undefined
      }, cellProps.style);

      var handleCellKeyDown = function handleCellKeyDown(event) {
        if (isExpandable) {
          if (_this2.state.popoverIsOpen) {
            return;
          }

          switch (event.key) {
            case keys.ENTER:
            case keys.F2:
              event.preventDefault();

              _this2.setState({
                popoverIsOpen: true
              });

              break;
          }
        } else {
          if (event.key === keys.ENTER || event.key === keys.F2 || event.key === keys.ESCAPE) {
            var interactables = _this2.getInteractables();

            if (interactables.length >= 2) {
              switch (event.key) {
                case keys.ENTER:
                  // `Enter` only activates the trap
                  if (_this2.state.isEntered === false) {
                    _this2.enableTabbing();

                    _this2.setState({
                      isEntered: true
                    }); // result of this keypress is focus shifts to the first interactive element
                    // and then the browser fires the onClick event because that's how [Enter] works
                    // so we need to prevent that default action otherwise entering the trap triggers the first element


                    event.preventDefault();
                  }

                  break;

                case keys.F2:
                  // toggle interactives' focus trap
                  _this2.setState(function (_ref2) {
                    var isEntered = _ref2.isEntered;

                    if (isEntered) {
                      _this2.preventTabbing();
                    } else {
                      _this2.enableTabbing();
                    }

                    return {
                      isEntered: !isEntered
                    };
                  });

                  break;

                case keys.ESCAPE:
                  // `Escape` only de-activates the trap
                  _this2.preventTabbing();

                  if (_this2.state.isEntered === true) {
                    _this2.setState({
                      isEntered: false
                    });
                  }

                  break;
              }
            }
          }
        }
      };

      var isDefinedHeight = !!(rowHeightUtils === null || rowHeightUtils === void 0 ? void 0 : rowHeightUtils.getRowHeightOption(rowIndex, rowHeightsOptions));

      var cellContentProps = _objectSpread(_objectSpread({}, rest), {}, {
        setCellProps: this.setCellProps,
        column: column,
        columnType: columnType,
        isExpandable: isExpandable,
        isExpanded: this.state.popoverIsOpen,
        isDetails: false,
        setCellContentsRef: this.setCellContentsRef,
        rowHeightsOptions: rowHeightsOptions,
        rowHeightUtils: rowHeightUtils,
        isDefinedHeight: isDefinedHeight
      });

      var anchorClass = 'euiDataGridRowCell__expandFlex';
      var expandClass = isDefinedHeight ? 'euiDataGridRowCell__contentByHeight' : 'euiDataGridRowCell__expandContent';

      var anchorContent = ___EmotionJSX(EuiFocusTrap, {
        disabled: !this.state.isEntered,
        autoFocus: true,
        onDeactivation: function onDeactivation() {
          _this2.setState({
            isEntered: false
          }, _this2.preventTabbing);
        },
        style: isDefinedHeight ? {
          height: '100%'
        } : {},
        clickOutsideDisables: true
      }, ___EmotionJSX("div", {
        className: anchorClass
      }, ___EmotionJSX("div", {
        className: expandClass
      }, ___EmotionJSX(EuiDataGridCellContent, cellContentProps))));

      if (isExpandable || column && column.cellActions) {
        if (showCellButtons) {
          anchorContent = ___EmotionJSX("div", {
            className: anchorClass
          }, ___EmotionJSX("div", {
            className: expandClass
          }, ___EmotionJSX(EuiDataGridCellContent, cellContentProps)), ___EmotionJSX(EuiDataGridCellButtons, {
            rowIndex: rowIndex,
            column: column,
            popoverIsOpen: this.state.popoverIsOpen,
            closePopover: this.closePopover,
            onExpandClick: function onExpandClick() {
              _this2.setState(function (_ref3) {
                var popoverIsOpen = _ref3.popoverIsOpen;
                return {
                  popoverIsOpen: !popoverIsOpen
                };
              });
            }
          }));
        } else {
          anchorContent = ___EmotionJSX("div", {
            className: anchorClass
          }, ___EmotionJSX("div", {
            className: expandClass
          }, ___EmotionJSX(EuiDataGridCellContent, cellContentProps)));
        }
      }

      var innerContent = anchorContent;

      if (isExpandable || column && column.cellActions) {
        if (this.state.popoverIsOpen) {
          innerContent = ___EmotionJSX("div", {
            className: isDefinedHeight ? 'euiDataGridRowCell__contentByHeight' : 'euiDataGridRowCell__content'
          }, ___EmotionJSX(EuiDataGridCellPopover, {
            anchorContent: anchorContent,
            cellContentProps: cellContentProps,
            cellContentsRef: this.cellContentsRef,
            closePopover: this.closePopover,
            column: column,
            panelRefFn: function panelRefFn(ref) {
              return _this2.popoverPanelRef.current = ref;
            },
            popoverIsOpen: this.state.popoverIsOpen,
            rowIndex: rowIndex,
            renderCellValue: rest.renderCellValue,
            popoverContent: PopoverContent
          }));
        } else {
          innerContent = anchorContent;
        }
      }

      var content = ___EmotionJSX("div", _extends({
        role: "gridcell",
        tabIndex: this.state.isFocused && !this.state.disableCellTabIndex ? 0 : -1,
        ref: this.cellRef
      }, cellProps, {
        "data-test-subj": "dataGridRowCell" // Data attributes to help target specific cells by either data or current cell location
        ,
        "data-gridcell-column-id": this.props.columnId // Static column ID name, not affected by column order
        ,
        "data-gridcell-column-index": this.props.colIndex // Affected by column reordering
        ,
        "data-gridcell-row-index": this.props.rowIndex // Index from data, not affected by sorting or pagination
        ,
        "data-gridcell-visible-row-index": this.props.visibleRowIndex // Affected by sorting & pagination
        ,
        "data-gridcell-id": "".concat(this.props.colIndex, ",").concat(this.props.rowIndex) // TODO: Deprecate in favor of the above 4 data attrs
        ,
        onKeyDown: handleCellKeyDown,
        onFocus: this.onFocus,
        onMouseEnter: function onMouseEnter() {
          _this2.setState({
            enableInteractions: true
          });
        },
        onMouseLeave: function onMouseLeave() {
          _this2.setState({
            enableInteractions: false
          });
        },
        onBlur: this.onBlur
      }), innerContent);

      return rowManager && !IS_JEST_ENVIRONMENT ? /*#__PURE__*/createPortal(content, rowManager.getRow(rowIndex)) : content;
    }
  }]);

  return EuiDataGridCell;
}(Component);

_defineProperty(EuiDataGridCell, "activeFocusTimeoutId", undefined);

_defineProperty(EuiDataGridCell, "contextType", DataGridFocusContext);