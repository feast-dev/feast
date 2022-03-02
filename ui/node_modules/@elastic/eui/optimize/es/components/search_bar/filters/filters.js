import _extends from "@babel/runtime/helpers/extends";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { IsFilter } from './is_filter';
import { FieldValueSelectionFilter } from './field_value_selection_filter';
import { FieldValueToggleFilter } from './field_value_toggle_filter';
import { FieldValueToggleGroupFilter } from './field_value_toggle_group_filter';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var createFilter = function createFilter(index, config, query, onChange) {
  var props = {
    index: index,
    query: query,
    onChange: onChange
  }; // We don't put `config` into `props` above because until we check
  // `config.type`, TS only knows that it's a `FilterConfig`, and that type
  // is used to define `props` as well. Once we've checked `config.type`
  // below, its type is narrowed correctly, hence we pass down `config`
  // separately.

  switch (config.type) {
    case 'is':
      return ___EmotionJSX(IsFilter, _extends({}, props, {
        config: config
      }));

    case 'field_value_selection':
      return ___EmotionJSX(FieldValueSelectionFilter, _extends({}, props, {
        config: config
      }));

    case 'field_value_toggle':
      return ___EmotionJSX(FieldValueToggleFilter, _extends({}, props, {
        config: config
      }));

    case 'field_value_toggle_group':
      return ___EmotionJSX(FieldValueToggleGroupFilter, _extends({}, props, {
        config: config
      }));

    default:
      // @ts-ignore TS knows that we've checked `config.type` exhaustively
      throw new Error("Unknown search filter type [".concat(config.type, "]"));
  }
};