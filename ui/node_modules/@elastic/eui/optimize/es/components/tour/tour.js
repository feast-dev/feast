import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useEuiTour } from './useEuiTour';
export var EuiTour = function EuiTour(_ref) {
  var children = _ref.children,
      steps = _ref.steps,
      initialState = _ref.initialState;

  var _useEuiTour = useEuiTour(steps, initialState),
      _useEuiTour2 = _slicedToArray(_useEuiTour, 3),
      stepProps = _useEuiTour2[0],
      actions = _useEuiTour2[1],
      state = _useEuiTour2[2];

  return children(stepProps, actions, state);
};