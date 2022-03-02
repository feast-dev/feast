"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useContainerCallbacks = exports.getPosition = exports.getPanelMinSize = exports.sizesOnly = exports.pxToPercent = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _react = require("react");

var _common = require("../common");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function isMouseEvent(event) {
  return (0, _typeof2.default)(event) === 'object' && 'pageX' in event && 'pageY' in event;
}

var pxToPercent = function pxToPercent(proportion, whole) {
  if (whole < 1 || proportion < 0) return 0;
  return proportion / whole * 100;
};

exports.pxToPercent = pxToPercent;

var sizesOnly = function sizesOnly(panelObject) {
  return Object.values(panelObject).reduce(function (out, panel) {
    out[panel.id] = panel.size;
    return out;
  }, {});
};

exports.sizesOnly = sizesOnly;

var _getPanelMinSize = function _getPanelMinSize(panelMinSize, containerSize) {
  var panelMinSizePercent = 0;
  var panelMinSizeInt = parseInt(panelMinSize);

  if (panelMinSize.indexOf('px') > -1) {
    panelMinSizePercent = pxToPercent(panelMinSizeInt, containerSize);
  } else if (panelMinSize.indexOf('%') > -1) {
    panelMinSizePercent = pxToPercent(containerSize * (panelMinSizeInt / 100), containerSize);
  }

  return panelMinSizePercent;
};

var getPanelMinSize = function getPanelMinSize(panelMinSize, containerSize) {
  var paddingMin = _getPanelMinSize(panelMinSize[1], containerSize);

  var configMin = _getPanelMinSize(panelMinSize[0], containerSize);

  return Math.max(configMin, paddingMin);
};

exports.getPanelMinSize = getPanelMinSize;

var getPosition = function getPosition(event, isHorizontal) {
  var clientX = isMouseEvent(event) ? event.clientX : event.touches[0].clientX;
  var clientY = isMouseEvent(event) ? event.clientY : event.touches[0].clientY;
  return isHorizontal ? clientX : clientY;
};

exports.getPosition = getPosition;

var getSiblingPanel = function getSiblingPanel(element, adjacency) {
  if (!element) return null;
  var method = adjacency === 'prev' ? 'previousElementSibling' : 'nextElementSibling';
  var sibling = element[method];

  while (sibling) {
    if (sibling.matches('.euiResizablePanel:not(.euiResizablePanel-isCollapsed)')) {
      return sibling;
    }

    sibling = sibling[method];
  }
}; // lazy initialization to prevent rerender on initial interaction


var init = function init(state) {
  return state;
};

var useContainerCallbacks = function useContainerCallbacks(_ref) {
  var initialState = _ref.initialState,
      containerRef = _ref.containerRef,
      onPanelWidthChange = _ref.onPanelWidthChange;

  function reducer(state, action) {
    var getContainerSize = function getContainerSize(isHorizontal) {
      return isHorizontal ? containerRef.current.getBoundingClientRect().width : containerRef.current.getBoundingClientRect().height;
    };

    var runSideEffect = function runSideEffect(panels) {
      if (onPanelWidthChange) {
        onPanelWidthChange(sizesOnly(panels));
      }
    };

    var withSideEffect = function withSideEffect(newState) {
      runSideEffect(newState.panels);
      return newState;
    };

    switch (action.type) {
      case 'EUI_RESIZABLE_CONTAINER_INIT':
        {
          var isHorizontal = action.payload.isHorizontal;
          return _objectSpread(_objectSpread({}, state), {}, {
            isHorizontal: isHorizontal,
            containerSize: getContainerSize(isHorizontal)
          });
        }

      case 'EUI_RESIZABLE_PANEL_REGISTER':
        {
          var panel = action.payload.panel;
          return _objectSpread(_objectSpread({}, state), {}, {
            panels: _objectSpread(_objectSpread({}, state.panels), {}, (0, _defineProperty2.default)({}, panel.id, panel))
          });
        }

      case 'EUI_RESIZABLE_PANEL_DEREGISTER':
        {
          var panelId = action.payload.panelId;
          return _objectSpread(_objectSpread({}, state), {}, {
            panels: Object.values(state.panels).reduce(function (out, panel) {
              if (panel.id !== panelId) {
                out[panel.id] = panel;
              }

              return out;
            }, {})
          });
        }

      case 'EUI_RESIZABLE_BUTTON_REGISTER':
        {
          var resizer = action.payload.resizer;
          return _objectSpread(_objectSpread({}, state), {}, {
            resizers: _objectSpread(_objectSpread({}, state.resizers), {}, (0, _defineProperty2.default)({}, resizer.id, resizer))
          });
        }

      case 'EUI_RESIZABLE_BUTTON_DEREGISTER':
        {
          var resizerId = action.payload.resizerId;
          return _objectSpread(_objectSpread({}, state), {}, {
            resizers: Object.values(state.resizers).reduce(function (out, panel) {
              if (panel.id !== resizerId) {
                out[panel.id] = panel;
              }

              return out;
            }, {})
          });
        }

      case 'EUI_RESIZABLE_DRAG_START':
        {
          var _action$payload = action.payload,
              position = _action$payload.position,
              prevPanelId = _action$payload.prevPanelId,
              nextPanelId = _action$payload.nextPanelId;
          return _objectSpread(_objectSpread({}, state), {}, {
            isDragging: true,
            currentResizerPos: position,
            prevPanelId: prevPanelId,
            nextPanelId: nextPanelId
          });
        }

      case 'EUI_RESIZABLE_DRAG_MOVE':
        {
          if (!state.isDragging) {
            return state;
          }

          var _action$payload2 = action.payload,
              _position = _action$payload2.position,
              _prevPanelId = _action$payload2.prevPanelId,
              _nextPanelId = _action$payload2.nextPanelId;
          var prevPanel = state.panels[_prevPanelId];
          var nextPanel = state.panels[_nextPanelId];
          var delta = _position - state.currentResizerPos;
          var prevPanelMin = getPanelMinSize(prevPanel.minSize, state.containerSize);
          var nextPanelMin = getPanelMinSize(nextPanel.minSize, state.containerSize);
          var prevPanelSize = pxToPercent(prevPanel.getSizePx() + delta, state.containerSize);
          var nextPanelSize = pxToPercent(nextPanel.getSizePx() - delta, state.containerSize);

          if (prevPanelSize >= prevPanelMin && nextPanelSize >= nextPanelMin) {
            var _objectSpread4;

            return withSideEffect(_objectSpread(_objectSpread({}, state), {}, {
              currentResizerPos: _position,
              panels: _objectSpread(_objectSpread({}, state.panels), {}, (_objectSpread4 = {}, (0, _defineProperty2.default)(_objectSpread4, _prevPanelId, _objectSpread(_objectSpread({}, state.panels[_prevPanelId]), {}, {
                size: prevPanelSize
              })), (0, _defineProperty2.default)(_objectSpread4, _nextPanelId, _objectSpread(_objectSpread({}, state.panels[_nextPanelId]), {}, {
                size: nextPanelSize
              })), _objectSpread4))
            }));
          }

          return state;
        }

      case 'EUI_RESIZABLE_KEY_MOVE':
        {
          var _action$payload3 = action.payload,
              _prevPanelId2 = _action$payload3.prevPanelId,
              _nextPanelId2 = _action$payload3.nextPanelId,
              direction = _action$payload3.direction;
          var _prevPanel = state.panels[_prevPanelId2];
          var _nextPanel = state.panels[_nextPanelId2];

          var _prevPanelMin = getPanelMinSize(_prevPanel.minSize, state.containerSize);

          var _nextPanelMin = getPanelMinSize(_nextPanel.minSize, state.containerSize);

          var _prevPanelSize = pxToPercent(_prevPanel.getSizePx() - (direction === 'backward' ? 10 : -10), state.containerSize);

          var _nextPanelSize = pxToPercent(_nextPanel.getSizePx() - (direction === 'forward' ? 10 : -10), state.containerSize);

          if (_prevPanelSize >= _prevPanelMin && _nextPanelSize >= _nextPanelMin) {
            var _objectSpread5;

            return withSideEffect(_objectSpread(_objectSpread({}, state), {}, {
              isDragging: false,
              panels: _objectSpread(_objectSpread({}, state.panels), {}, (_objectSpread5 = {}, (0, _defineProperty2.default)(_objectSpread5, _prevPanelId2, _objectSpread(_objectSpread({}, state.panels[_prevPanelId2]), {}, {
                size: _prevPanelSize
              })), (0, _defineProperty2.default)(_objectSpread5, _nextPanelId2, _objectSpread(_objectSpread({}, state.panels[_nextPanelId2]), {}, {
                size: _nextPanelSize
              })), _objectSpread5))
            }));
          }

          return state;
        }

      case 'EUI_RESIZABLE_TOGGLE':
        {
          var _action$payload4 = action.payload,
              options = _action$payload4.options,
              currentPanelId = _action$payload4.panelId;
          var currentPanel = state.panels[currentPanelId];
          var shouldCollapse = !currentPanel.isCollapsed;
          var panelElement = document.getElementById(currentPanelId);
          var prevResizer = panelElement.previousElementSibling;

          var _prevPanel2 = prevResizer ? prevResizer.previousElementSibling : null;

          var nextResizer = panelElement.nextElementSibling;

          var _nextPanel2 = nextResizer ? nextResizer.nextElementSibling : null;

          var resizersToDisable = {};

          if (prevResizer && _prevPanel2) {
            resizersToDisable[prevResizer.id] = state.panels[_prevPanel2.id].isCollapsed ? true : shouldCollapse;
          }

          if (nextResizer && _nextPanel2) {
            resizersToDisable[nextResizer.id] = state.panels[_nextPanel2.id].isCollapsed ? true : shouldCollapse;
          }

          var otherPanels = {};

          if (_prevPanel2 && !state.panels[_prevPanel2.id].isCollapsed && options.direction === 'right') {
            otherPanels[_prevPanel2.id] = state.panels[_prevPanel2.id];
          }

          if (_nextPanel2 && !state.panels[_nextPanel2.id].isCollapsed && options.direction === 'left') {
            otherPanels[_nextPanel2.id] = state.panels[_nextPanel2.id];
          }

          var siblings = Object.keys(otherPanels).length; // A toggling sequence has occurred where an immediate sibling panel
          // has not been found. We need to move more broadly through the DOM
          // to find the next most suitable panel or space affordance.
          // Can only occur when multiple immediate sibling panels are collapsed.

          if (!siblings) {
            var maybePrevPanel = getSiblingPanel(panelElement, 'prev');
            var maybeNextPanel = getSiblingPanel(panelElement, 'next');
            var validPrevPanel = maybePrevPanel ? state.panels[maybePrevPanel.id] : null;
            var validNextPanel = maybeNextPanel ? state.panels[maybeNextPanel.id] : null; // Intentional, preferential redistribution order

            if (validPrevPanel && options.direction === 'right') {
              otherPanels[validPrevPanel.id] = validPrevPanel;
            } else if (validNextPanel && options.direction === 'left') {
              otherPanels[validNextPanel.id] = validNextPanel;
            } else {
              if (validPrevPanel) otherPanels[validPrevPanel.id] = validPrevPanel;
              if (validNextPanel) otherPanels[validNextPanel.id] = validNextPanel;
            }

            siblings = Object.keys(otherPanels).length;
          }

          var newPanelSize = shouldCollapse ? pxToPercent(!currentPanel.mode ? 0 : 24, // size of the default toggle button
          state.containerSize) : currentPanel.prevSize;

          var _delta = shouldCollapse ? (currentPanel.size - newPanelSize) / siblings : (newPanelSize - currentPanel.size) / siblings * -1;

          var collapsedPanelsSize = Object.values(state.panels).reduce(function (sum, panel) {
            if (panel.id !== currentPanelId && panel.isCollapsed) {
              sum += panel.size;
            }

            return sum;
          }, 0); // A toggling sequence has occurred where a to-be-opened panel will
          // become the only open panel. Rather than reopen to its previous
          // size, give it the full width, less size occupied by collapsed panels.
          // Can only occur with external toggling.

          if (!shouldCollapse && !siblings) {
            newPanelSize = 100 - collapsedPanelsSize;
          }

          var updatedPanels = {};

          if (_delta < 0 && Object.values(otherPanels).some(function (panel) {
            return panel.size + _delta < getPanelMinSize(panel.minSize, state.containerSize);
          })) {
            // A toggling sequence has occurred where a to-be-opened panel is
            // requesting more space than its logical sibling panel can afford.
            // Rather than choose another single panel to sacrifice space,
            // or try to pull proportionally from all availble panels
            // (neither of which is guaranteed to prevent negative resulting widths),
            // or attempt something even more complex,
            // we redistribute _all_ space evenly to non-collapsed panels
            // as something of a reset.
            // This situation can only occur when (n-1) panels are collapsed at once
            // and the most recently collapsed panel gains significant width
            // during the previously occurring collapse.
            // That is (largely), external toggling where the default logic has
            // been negated by the lack of panel mode distinction.
            otherPanels = Object.values(state.panels).reduce(function (out, panel) {
              if (panel.id !== currentPanelId && !panel.isCollapsed) {
                out[panel.id] = _objectSpread({}, panel);
              }

              return out;
            }, {});
            newPanelSize = (100 - collapsedPanelsSize) / (Object.keys(otherPanels).length + 1);
            updatedPanels = Object.values(otherPanels).reduce(function (out, panel) {
              out[panel.id] = _objectSpread(_objectSpread({}, panel), {}, {
                size: newPanelSize
              });
              return out;
            }, {});
          } else {
            // A toggling sequence has occurred that is standard and predictable
            updatedPanels = Object.values(otherPanels).reduce(function (out, panel) {
              out[panel.id] = _objectSpread(_objectSpread({}, panel), {}, {
                size: panel.size + _delta
              });
              return out;
            }, {});
          }

          return withSideEffect(_objectSpread(_objectSpread({}, state), {}, {
            panels: _objectSpread(_objectSpread(_objectSpread({}, state.panels), updatedPanels), {}, (0, _defineProperty2.default)({}, currentPanelId, _objectSpread(_objectSpread({}, state.panels[currentPanelId]), {}, {
              size: newPanelSize,
              isCollapsed: shouldCollapse,
              prevSize: shouldCollapse ? currentPanel.size : newPanelSize
            }))),
            resizers: Object.values(state.resizers).reduce(function (out, resizer) {
              var _resizersToDisable$re;

              out[resizer.id] = _objectSpread(_objectSpread({}, resizer), {}, {
                isFocused: false,
                isDisabled: (_resizersToDisable$re = resizersToDisable[resizer.id]) !== null && _resizersToDisable$re !== void 0 ? _resizersToDisable$re : resizer.isDisabled
              });
              return out;
            }, {})
          }));
        }

      case 'EUI_RESIZABLE_BUTTON_FOCUS':
        {
          var _resizerId = action.payload.resizerId;
          return _objectSpread(_objectSpread({}, state), {}, {
            resizers: Object.values(state.resizers).reduce(function (out, resizer) {
              out[resizer.id] = _objectSpread(_objectSpread({}, resizer), {}, {
                isFocused: resizer.id === _resizerId
              });
              return out;
            }, {})
          });
        }

      case 'EUI_RESIZABLE_BUTTON_BLUR':
        {
          return _objectSpread(_objectSpread({}, state), {}, {
            resizers: Object.values(state.resizers).reduce(function (out, resizer) {
              out[resizer.id] = _objectSpread(_objectSpread({}, resizer), {}, {
                isFocused: false
              });
              return out;
            }, {})
          });
        }

      case 'EUI_RESIZABLE_RESET':
        {
          return _objectSpread(_objectSpread({}, initialState), {}, {
            panels: state.panels,
            resizers: state.resizers,
            containerSize: state.containerSize
          });
        }

      case 'EUI_RESIZABLE_ONCHANGE':
        {
          onPanelWidthChange(sizesOnly(state.panels));
          return state;
        }
      // TODO: Implement more generic version of
      // 'EUI_RESIZABLE_DRAG_MOVE' to expose to consumers

      case 'EUI_RESIZABLE_RESIZE':
        {
          return state;
        }

      default:
        (0, _common.assertNever)(action);
        return state;
    }
  }

  var _useReducer = (0, _react.useReducer)(reducer, initialState, init),
      _useReducer2 = (0, _slicedToArray2.default)(_useReducer, 2),
      reducerState = _useReducer2[0],
      dispatch = _useReducer2[1];

  var actions = (0, _react.useMemo)(function () {
    return {
      reset: function reset() {
        return dispatch({
          type: 'EUI_RESIZABLE_RESET'
        });
      },
      initContainer: function initContainer(isHorizontal) {
        return dispatch({
          type: 'EUI_RESIZABLE_CONTAINER_INIT',
          payload: {
            isHorizontal: isHorizontal
          }
        });
      },
      registerPanel: function registerPanel(panel) {
        return dispatch({
          type: 'EUI_RESIZABLE_PANEL_REGISTER',
          payload: {
            panel: panel
          }
        });
      },
      deregisterPanel: function deregisterPanel(panelId) {
        return dispatch({
          type: 'EUI_RESIZABLE_PANEL_DEREGISTER',
          payload: {
            panelId: panelId
          }
        });
      },
      registerResizer: function registerResizer(resizer) {
        return dispatch({
          type: 'EUI_RESIZABLE_BUTTON_REGISTER',
          payload: {
            resizer: resizer
          }
        });
      },
      deregisterResizer: function deregisterResizer(resizerId) {
        return dispatch({
          type: 'EUI_RESIZABLE_BUTTON_DEREGISTER',
          payload: {
            resizerId: resizerId
          }
        });
      },
      dragStart: function dragStart(_ref2) {
        var prevPanelId = _ref2.prevPanelId,
            nextPanelId = _ref2.nextPanelId,
            position = _ref2.position;
        return dispatch({
          type: 'EUI_RESIZABLE_DRAG_START',
          payload: {
            position: position,
            prevPanelId: prevPanelId,
            nextPanelId: nextPanelId
          }
        });
      },
      dragMove: function dragMove(_ref3) {
        var prevPanelId = _ref3.prevPanelId,
            nextPanelId = _ref3.nextPanelId,
            position = _ref3.position;
        return dispatch({
          type: 'EUI_RESIZABLE_DRAG_MOVE',
          payload: {
            position: position,
            prevPanelId: prevPanelId,
            nextPanelId: nextPanelId
          }
        });
      },
      keyMove: function keyMove(_ref4) {
        var prevPanelId = _ref4.prevPanelId,
            nextPanelId = _ref4.nextPanelId,
            direction = _ref4.direction;
        return dispatch({
          type: 'EUI_RESIZABLE_KEY_MOVE',
          payload: {
            prevPanelId: prevPanelId,
            nextPanelId: nextPanelId,
            direction: direction
          }
        });
      },
      togglePanel: function togglePanel(panelId, options) {
        return dispatch({
          type: 'EUI_RESIZABLE_TOGGLE',
          payload: {
            panelId: panelId,
            options: options
          }
        });
      },
      resizerFocus: function resizerFocus(resizerId) {
        return dispatch({
          type: 'EUI_RESIZABLE_BUTTON_FOCUS',
          payload: {
            resizerId: resizerId
          }
        });
      },
      resizerBlur: function resizerBlur() {
        return dispatch({
          type: 'EUI_RESIZABLE_BUTTON_BLUR'
        });
      }
    };
  }, []);
  return [actions, reducerState];
};

exports.useContainerCallbacks = useContainerCallbacks;