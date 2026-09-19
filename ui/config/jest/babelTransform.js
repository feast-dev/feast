"use strict";

const babelJest = require("babel-jest").default;

const hasJsxRuntime = (() => {
  if (process.env.DISABLE_NEW_JSX_TRANSFORM === "true") {
    return false;
  }

  try {
    require.resolve("react/jsx-runtime");
    return true;
  } catch (e) {
    return false;
  }
})();

const transformImportMetaForJest = ({ types: t }) => ({
  visitor: {
    MetaProperty(path) {
      if (
        path.node.meta.name === "import" &&
        path.node.property.name === "meta"
      ) {
        // Jest runs transformed code as CommonJS, where import.meta is unavailable.
        path.replaceWith(t.objectExpression([]));
      }
    },
  },
});

module.exports = babelJest.createTransformer({
  presets: [
    [
      require.resolve("babel-preset-react-app"),
      {
        runtime: hasJsxRuntime ? "automatic" : "classic",
      },
    ],
  ],
  plugins: [transformImportMetaForJest],
  babelrc: false,
  configFile: false,
});
