'use strict'

var toH = require('hast-to-hyperscript')
var tableCellStyle = require('@mapbox/hast-util-table-cell-style')

module.exports = rehypeReact

var own = {}.hasOwnProperty
var tableElements = new Set([
  'table',
  'thead',
  'tbody',
  'tfoot',
  'tr',
  'th',
  'td'
])

// Add a React compiler.
function rehypeReact(options) {
  var settings = options || {}
  var createElement = settings.createElement

  this.Compiler = compiler

  function compiler(node) {
    var result = toH(h, tableCellStyle(node), settings.prefix)

    if (node.type === 'root') {
      // Invert <https://github.com/syntax-tree/hast-to-hyperscript/blob/d227372/index.js#L46-L56>.
      result =
        result.type === 'div' &&
        (node.children.length !== 1 || node.children[0].type !== 'element')
          ? result.props.children
          : [result]

      return createElement(settings.Fragment || 'div', {}, result)
    }

    return result
  }

  // Wrap `createElement` to pass components in.
  function h(name, props, children) {
    var component = name

    // Currently, a warning is triggered by react for *any* white space in
    // tables.
    // So we remove the pretty lines for now.
    // See: <https://github.com/facebook/react/pull/7081>.
    // See: <https://github.com/facebook/react/pull/7515>.
    // See: <https://github.com/remarkjs/remark-react/issues/64>.
    if (children && tableElements.has(name)) {
      children = children.filter(function (child) {
        return child !== '\n'
      })
    }

    if (settings.components && own.call(settings.components, name)) {
      component = settings.components[name]

      if (settings.passNode) {
        props.node = this
      }
    }

    return createElement(component, props, children)
  }
}
