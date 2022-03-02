module.exports = [
  require('./webpack-protocol'),
  require('./webpack-bootstrap'),
  require('./bower-component'),
  require('./npm-module'),
  /* insert here any additional special character CODECs */
  require('./output-relative'),
  require('./output-root-relative'),
  require('./project-relative'),
  require('./project-root-relative'),
  require('./source-relative'),
  require('./source-root-relative'),
  require('./absolute')
];
