'use strict'

module.exports = breaks

var visit = require('unist-util-visit')

var find = /[\t ]*(?:\r?\n|\r)/g

var splice = [].splice

function breaks() {
  return transform
}

function transform(tree) {
  visit(tree, 'text', ontext)
}

function ontext(node, index, parent) {
  var result = []
  var start = 0
  var match
  var position

  find.lastIndex = 0

  match = find.exec(node.value)

  while (match) {
    position = match.index

    if (start !== position) {
      result.push({type: 'text', value: node.value.slice(start, position)})
    }

    result.push({type: 'break'})
    start = position + match[0].length
    match = find.exec(node.value)
  }

  if (result.length > 0) {
    if (start < node.value.length) {
      result.push({type: 'text', value: node.value.slice(start)})
    }

    splice.apply(parent.children, [index, 1].concat(result))
    return index + result.length
  }
}
