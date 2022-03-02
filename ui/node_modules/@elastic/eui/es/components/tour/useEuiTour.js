function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useReducer } from 'react';
import { assertNever } from '../common';
export var useEuiTour = function useEuiTour(stepsArray, initialState) {
  function reducer(state, action) {
    switch (action.type) {
      case 'EUI_TOUR_FINISH':
        {
          var currentTourStep = action.payload.resetTour ? 1 : state.currentTourStep;
          return _objectSpread(_objectSpread({}, state), {}, {
            currentTourStep: currentTourStep,
            isTourActive: false
          });
        }

      case 'EUI_TOUR_RESET':
        return _objectSpread(_objectSpread({}, state), {}, {
          currentTourStep: 1,
          isTourActive: true
        });

      case 'EUI_TOUR_NEXT':
        {
          var nextStep = state.currentTourStep === stepsArray.length ? state.currentTourStep : state.currentTourStep + 1;
          return _objectSpread(_objectSpread({}, state), {}, {
            currentTourStep: nextStep
          });
        }

      case 'EUI_TOUR_PREVIOUS':
        {
          var prevStep = state.currentTourStep === 1 ? state.currentTourStep : state.currentTourStep - 1;
          return _objectSpread(_objectSpread({}, state), {}, {
            currentTourStep: prevStep
          });
        }

      case 'EUI_TOUR_GOTO':
        {
          var step = action.payload.step;
          var isTourActive = typeof action.payload.isTourActive !== 'undefined' ? action.payload.isTourActive : state.isTourActive;
          var goTo = step <= stepsArray.length && step > 0 ? step : state.currentTourStep;
          return _objectSpread(_objectSpread({}, state), {}, {
            currentTourStep: goTo,
            isTourActive: isTourActive
          });
        }

      default:
        assertNever(action);
        return state;
    }
  }

  var _useReducer = useReducer(reducer, initialState),
      _useReducer2 = _slicedToArray(_useReducer, 2),
      state = _useReducer2[0],
      dispatch = _useReducer2[1];

  var actions = {
    finishTour: function finishTour() {
      var resetTour = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : true;
      return dispatch({
        type: 'EUI_TOUR_FINISH',
        payload: {
          resetTour: resetTour
        }
      });
    },
    resetTour: function resetTour() {
      return dispatch({
        type: 'EUI_TOUR_RESET'
      });
    },
    decrementStep: function decrementStep() {
      return dispatch({
        type: 'EUI_TOUR_PREVIOUS'
      });
    },
    incrementStep: function incrementStep() {
      return dispatch({
        type: 'EUI_TOUR_NEXT'
      });
    },
    goToStep: function goToStep(step, isTourActive) {
      return dispatch({
        type: 'EUI_TOUR_GOTO',
        payload: {
          step: step,
          isTourActive: isTourActive
        }
      });
    }
  };
  var steps = stepsArray.map(function (step) {
    return _objectSpread(_objectSpread({}, step), {}, {
      isStepOpen: state.currentTourStep === step.step && state.isTourActive,
      minWidth: state.tourPopoverWidth,
      onFinish: actions.finishTour,
      stepsTotal: stepsArray.length,
      subtitle: state.tourSubtitle
    });
  });
  return [steps, actions, state];
};