import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component, createContext } from 'react';
import classNames from 'classnames';
import { EuiI18n } from '../i18n';
import { EuiIcon } from '../icon';
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiText } from '../text';
import { keys, htmlIdGenerator } from '../../services';
import { EuiInnerText } from '../inner_text';
import { jsx as ___EmotionJSX } from "@emotion/react";
var EuiTreeViewContext = /*#__PURE__*/createContext('');

function hasAriaLabel(x) {
  return x.hasOwnProperty('aria-label');
}

function getTreeId(propId, contextId, idGenerator) {
  return propId !== null && propId !== void 0 ? propId : contextId === '' ? idGenerator() : contextId;
}

var displayToClassNameMap = {
  default: null,
  compressed: 'euiTreeView--compressed'
};
export var EuiTreeView = /*#__PURE__*/function (_Component) {
  _inherits(EuiTreeView, _Component);

  var _super = _createSuper(EuiTreeView);

  function EuiTreeView() {
    var _this;

    _classCallCheck(this, EuiTreeView);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "treeIdGenerator", htmlIdGenerator('euiTreeView'));

    _defineProperty(_assertThisInitialized(_this), "isNested", !!_this.context);

    _defineProperty(_assertThisInitialized(_this), "state", {
      openItems: _this.props.expandByDefault ? _this.props.items.map(function (_ref) {
        var id = _ref.id,
            children = _ref.children;
        return children ? id : null;
      }).filter(function (x) {
        return x != null;
      }) : _this.props.items.map(function (_ref2) {
        var id = _ref2.id,
            children = _ref2.children,
            isExpanded = _ref2.isExpanded;
        return children && isExpanded ? id : null;
      }).filter(function (x) {
        return x != null;
      }),
      activeItem: '',
      treeID: getTreeId(_this.props.id, _this.context, _this.treeIdGenerator),
      expandChildNodes: _this.props.expandByDefault || false
    });

    _defineProperty(_assertThisInitialized(_this), "buttonRef", []);

    _defineProperty(_assertThisInitialized(_this), "setButtonRef", function (ref, index) {
      _this.buttonRef[index] = ref;
    });

    _defineProperty(_assertThisInitialized(_this), "handleNodeClick", function (node) {
      var ignoreCallback = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;

      var index = _this.state.openItems.indexOf(node.id);

      _this.setState({
        expandChildNodes: false
      });

      node.isExpanded = !node.isExpanded;

      if (!ignoreCallback && node.callback !== undefined) {
        node.callback();
      }

      if (_this.isNodeOpen(node)) {
        // if the node is part of openItems[] then remove it
        _this.setState({
          openItems: _this.state.openItems.filter(function (_, i) {
            return i !== index;
          })
        });
      } else {
        // if the node isn't part of openItems[] then add it
        _this.setState(function (prevState) {
          return {
            openItems: [].concat(_toConsumableArray(prevState.openItems), [node.id]),
            activeItem: node.id
          };
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "isNodeOpen", function (node) {
      return _this.state.openItems.includes(node.id);
    });

    _defineProperty(_assertThisInitialized(_this), "onKeyDown", function (event, node) {
      switch (event.key) {
        case keys.ARROW_DOWN:
          {
            var nodeButtons = Array.from(document.querySelectorAll("[data-test-subj=\"euiTreeViewButton-".concat(_this.state.treeID, "\"]")));
            var currentIndex = nodeButtons.indexOf(event.currentTarget);

            if (currentIndex > -1) {
              var nextButton = nodeButtons[currentIndex + 1];

              if (nextButton) {
                event.preventDefault();
                event.stopPropagation();
                nextButton.focus();
              }
            }

            break;
          }

        case keys.ARROW_UP:
          {
            var _nodeButtons = Array.from(document.querySelectorAll("[data-test-subj=\"euiTreeViewButton-".concat(_this.state.treeID, "\"]")));

            var _currentIndex = _nodeButtons.indexOf(event.currentTarget);

            if (_currentIndex > -1) {
              var prevButton = _nodeButtons[_currentIndex + -1];

              if (prevButton) {
                event.preventDefault();
                event.stopPropagation();
                prevButton.focus();
              }
            }

            break;
          }

        case keys.ARROW_RIGHT:
          {
            if (!_this.isNodeOpen(node)) {
              event.preventDefault();
              event.stopPropagation();

              _this.handleNodeClick(node, true);
            }

            break;
          }

        case keys.ARROW_LEFT:
          {
            if (_this.isNodeOpen(node)) {
              event.preventDefault();
              event.stopPropagation();

              _this.handleNodeClick(node, true);
            }
          }

        default:
          break;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onChildrenKeydown", function (event, index) {
      if (event.key === keys.ARROW_LEFT) {
        event.preventDefault();
        event.stopPropagation();

        _this.buttonRef[index].focus();
      }
    });

    return _this;
  }

  _createClass(EuiTreeView, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (this.props.id !== prevProps.id) {
        // eslint-disable-next-line react/no-did-update-set-state
        this.setState({
          treeID: getTreeId(this.props.id, this.context, this.treeIdGenerator)
        });
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          children = _this$props.children,
          className = _this$props.className,
          items = _this$props.items,
          _this$props$display = _this$props.display,
          display = _this$props$display === void 0 ? 'default' : _this$props$display,
          expandByDefault = _this$props.expandByDefault,
          showExpansionArrows = _this$props.showExpansionArrows,
          rest = _objectWithoutProperties(_this$props, ["children", "className", "items", "display", "expandByDefault", "showExpansionArrows"]); // Computed classNames


      var classes = classNames('euiTreeView', display ? displayToClassNameMap[display] : null, {
        'euiTreeView--withArrows': showExpansionArrows
      }, className);
      var instructionsId = "".concat(this.state.treeID, "--instruction");
      return ___EmotionJSX(EuiTreeViewContext.Provider, {
        value: this.state.treeID
      }, ___EmotionJSX(EuiText, {
        size: display === 'compressed' ? 's' : 'm',
        className: "euiTreeView__wrapper"
      }, !this.isNested && ___EmotionJSX(EuiI18n, {
        token: "euiTreeView.listNavigationInstructions",
        default: "You can quickly navigate this list using arrow keys."
      }, function (listNavigationInstructions) {
        return ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
          id: instructionsId
        }, listNavigationInstructions));
      }), ___EmotionJSX("ul", _extends({
        className: classes,
        id: !this.isNested ? this.state.treeID : undefined,
        "aria-describedby": !this.isNested ? instructionsId : undefined
      }, rest), items.map(function (node, index) {
        var buttonId = node.id;

        var wrappingId = _this2.treeIdGenerator(buttonId);

        return ___EmotionJSX(EuiInnerText, {
          key: node.id + index,
          fallback: typeof node.label === 'string' ? node.label : ''
        }, function (ref, innerText) {
          return ___EmotionJSX(EuiI18n, {
            key: node.id + index,
            token: "euiTreeView.ariaLabel",
            default: "{nodeLabel} child of {ariaLabel}",
            values: {
              nodeLabel: innerText,
              ariaLabel: hasAriaLabel(rest) ? rest['aria-label'] : ''
            }
          }, function (ariaLabel) {
            var label = hasAriaLabel(rest) ? {
              'aria-label': ariaLabel
            } : {
              'aria-labelledby': "".concat(buttonId, " ").concat(rest['aria-labelledby'])
            };
            var nodeClasses = classNames('euiTreeView__node', display ? displayToClassNameMap[display] : null, {
              'euiTreeView__node--expanded': _this2.isNodeOpen(node)
            });
            var nodeButtonClasses = classNames('euiTreeView__nodeInner', showExpansionArrows && node.children ? 'euiTreeView__nodeInner--withArrows' : null, _this2.state.activeItem === node.id ? 'euiTreeView__node--active' : null, node.className ? node.className : null);
            return ___EmotionJSX(React.Fragment, null, ___EmotionJSX("li", {
              className: nodeClasses
            }, ___EmotionJSX("button", {
              id: buttonId,
              "aria-controls": wrappingId,
              "aria-expanded": _this2.isNodeOpen(node),
              ref: function ref(_ref3) {
                return _this2.setButtonRef(_ref3, index);
              },
              "data-test-subj": "euiTreeViewButton-".concat(_this2.state.treeID),
              onKeyDown: function onKeyDown(event) {
                return _this2.onKeyDown(event, node);
              },
              onClick: function onClick() {
                return _this2.handleNodeClick(node);
              },
              className: nodeButtonClasses
            }, showExpansionArrows && node.children ? ___EmotionJSX(EuiIcon, {
              className: "euiTreeView__expansionArrow",
              size: display === 'compressed' ? 's' : 'm',
              type: _this2.isNodeOpen(node) ? 'arrowDown' : 'arrowRight'
            }) : null, node.icon && !node.useEmptyIcon ? ___EmotionJSX("span", {
              className: "euiTreeView__iconWrapper"
            }, _this2.isNodeOpen(node) && node.iconWhenExpanded ? node.iconWhenExpanded : node.icon) : null, node.useEmptyIcon && !node.icon ? ___EmotionJSX("span", {
              className: "euiTreeView__iconPlaceholder"
            }) : null, ___EmotionJSX("span", {
              ref: ref,
              className: "euiTreeView__nodeLabel"
            }, node.label)), ___EmotionJSX("div", {
              id: wrappingId,
              onKeyDown: function onKeyDown(event) {
                return _this2.onChildrenKeydown(event, index);
              }
            }, node.children && _this2.isNodeOpen(node) ? ___EmotionJSX(EuiTreeView, _extends({
              items: node.children,
              display: display,
              showExpansionArrows: showExpansionArrows,
              expandByDefault: _this2.state.expandChildNodes
            }, label)) : null)));
          });
        });
      }))));
    }
  }]);

  return EuiTreeView;
}(Component);

_defineProperty(EuiTreeView, "contextType", EuiTreeViewContext);