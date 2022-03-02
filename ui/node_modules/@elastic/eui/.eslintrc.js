const SSPL_ELASTIC_2_0_LICENSE_HEADER = `
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
`;

module.exports = {
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaFeatures: {
      jsx: true,
    },
  },
  settings: {
    'import/resolver': {
      node: {
        extensions: ['.ts', '.tsx', '.js', '.json'],
      },
      webpack: {
        config: './src-docs/webpack.config.js',
      },
    },
    react: {
      version: 'detect',
    },
  },
  extends: [
    '@elastic/eslint-config-kibana',
    'plugin:@typescript-eslint/recommended',
    // Prettier options need to come last, in order to override other style
    // rules.
    'prettier/react',
    'prettier/standard',
    'plugin:prettier/recommended',
  ],
  plugins: ['jsx-a11y', 'prettier', 'local', 'react-hooks', '@emotion'],
  rules: {
    'prefer-template': 'error',
    'local/i18n': 'error',
    'local/href-with-rel': 'error',
    'local/forward-ref': 'error',
    'local/require-license-header': [
      'warn',
      {
        license: SSPL_ELASTIC_2_0_LICENSE_HEADER,
      },
    ],
    'no-use-before-define': 'off',
    quotes: ['warn', 'single', 'avoid-escape'],
    camelcase: 'off',
    'jsx-a11y/accessible-emoji': 'error',
    'jsx-a11y/alt-text': 'error',
    'jsx-a11y/anchor-has-content': 'error',
    'jsx-a11y/aria-activedescendant-has-tabindex': 'error',
    'jsx-a11y/aria-props': 'error',
    'jsx-a11y/aria-proptypes': 'error',
    'jsx-a11y/aria-role': [2, { ignoreNonDOM: true }],
    'jsx-a11y/aria-unsupported-elements': 'error',
    'jsx-a11y/heading-has-content': 'error',
    'jsx-a11y/html-has-lang': 'error',
    'jsx-a11y/iframe-has-title': 'error',
    'jsx-a11y/interactive-supports-focus': 'error',
    'jsx-a11y/media-has-caption': 'error',
    'jsx-a11y/mouse-events-have-key-events': 'error',
    'jsx-a11y/no-access-key': 'error',
    'jsx-a11y/no-distracting-elements': 'error',
    'jsx-a11y/no-interactive-element-to-noninteractive-role': 'error',
    'jsx-a11y/no-noninteractive-element-interactions': 'error',
    'jsx-a11y/no-noninteractive-element-to-interactive-role': 'error',
    'jsx-a11y/no-redundant-roles': 'error',
    'jsx-a11y/role-has-required-aria-props': 'error',
    'jsx-a11y/role-supports-aria-props': 'error',
    'jsx-a11y/scope': 'error',
    'jsx-a11y/tabindex-no-positive': 'error',
    'jsx-a11y/label-has-associated-control': 'error',

    'react-hooks/rules-of-hooks': 'error',
    'react-hooks/exhaustive-deps': 'warn',

    '@typescript-eslint/array-type': ['error', { default: 'array-simple' }],
    '@typescript-eslint/ban-types': 'off',
    '@typescript-eslint/explicit-function-return-type': 'off',
    '@typescript-eslint/explicit-member-accessibility': 'off',
    '@typescript-eslint/indent': 'off',
    '@typescript-eslint/ban-tslint-comment': 'error',
    '@typescript-eslint/no-empty-interface': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
    '@typescript-eslint/no-non-null-assertion': 'off',
    '@typescript-eslint/no-parameter-properties': 'off',
    '@typescript-eslint/no-triple-slash-reference': 'off',
    '@typescript-eslint/no-unused-vars': [
      'error',
      { argsIgnorePattern: '^_', ignoreRestSiblings: true },
    ],
    '@typescript-eslint/no-use-before-define': 'off',
    '@typescript-eslint/no-empty-function': 'off',
    // It"s all very well saying that some types are trivially inferrable,
    // but being explicit is still clearer.
    '@typescript-eslint/no-inferrable-types': 'off',
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    '@typescript-eslint/naming-convention': 'off',
    '@typescript-eslint/ban-ts-comment': [
      'error',
      {
        'ts-ignore': 'allow-with-description',
        'ts-expect-error': 'allow-with-description',
      },
    ],
  },
  env: {
    jest: true,
  },
  overrides: [
    {
      files: ['*.d.ts'],
      rules: {
        'react/no-multi-comp': 'off',
        'react/prefer-es6-class': 'off',
        'react/prefer-stateless-function': 'off',
      },
    },
    {
      globals: {
        cy: true,
      },
      files: ['*.spec.js'],
    },
  ],
};
