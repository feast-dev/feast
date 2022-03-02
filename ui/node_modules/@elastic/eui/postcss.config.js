module.exports = {
  plugins: [
    require('autoprefixer')(), // `browsers` property set via `.browserslistrc`
    require('postcss-inline-svg')({ relative: true, path: __dirname }),
  ],
};
