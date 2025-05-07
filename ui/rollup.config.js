import pluginTypescript from "@rollup/plugin-typescript";
import pluginCommonjs from "@rollup/plugin-commonjs";
import pluginNodeResolve from "@rollup/plugin-node-resolve";
import { babel } from "@rollup/plugin-babel";
import json from "@rollup/plugin-json";
import css from "rollup-plugin-import-css";
import svg from "rollup-plugin-svg";
import copy from "rollup-plugin-copy"; // https://npm.io/package/rollup-plugin-copy

import * as path from "path";
import pkg from "./package.json";

const moduleName = pkg.name.replace(/^@.*\//, "");
const inputFileName = "src/FeastUI.tsx";
const author = pkg.author;
const banner = `
  /**
   * @license
   * author: ${author}
   * ${moduleName}.js v${pkg.version}
   * Released under the ${pkg.license} license.
   */
`;

const rollupConfig = [
  {
    input: inputFileName,
    output: [
      {
        dir: "dist",
        entryFileNames: path.basename(pkg.module),
        format: "es",
        sourcemap: "inline",
        banner,
        exports: "named",
        inlineDynamicImports: true,
      },
      // CommonJS
      {
        dir: "dist",
        entryFileNames: path.basename(pkg.main),
        format: "cjs",
        sourcemap: "inline",
        banner,
        exports: "default",
        inlineDynamicImports: true,
      }
    ],
    external: [
      ...Object.keys(pkg.dependencies || {}),
      ...Object.keys(pkg.devDependencies || {}),
    ],
    plugins: [
      pluginTypescript(),
      pluginCommonjs({
        extensions: [".js", ".ts"],
      }),
      babel({
        babelHelpers: "bundled",
        configFile: path.resolve(__dirname, ".babelrc.js"),
      }),
      pluginNodeResolve({
        browser: false,
      }),
      css({
        output: "feast-ui.css",
        minify: true,
        inject: false,
      }),
      svg(),
      json(),
      copy({
        targets: [{ src: "src/assets/**/*", dest: "dist/assets/" }],
      }),
    ],
  }
];

export default rollupConfig;
