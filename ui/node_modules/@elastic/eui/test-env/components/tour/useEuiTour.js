"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useEuiTour = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

var _common = require("../common");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var useEuiTour = function useEuiTour(stepsArray, initialState) {
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
        (0, _common.assertNever)(action);
        return state;
    }
  }

  var _useReducer = (0, _react.useReducer)(reducer, initialState),
      _useReducer2 = (0, _slicedToArray2.default)(_useReducer, 2),
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

exports.useEuiTour = useEuiTour;