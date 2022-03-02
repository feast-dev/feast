'use strict'

module.exports = hoon
hoon.displayName = 'hoon'
hoon.aliases = []
function hoon(Prism) {
  Prism.languages.hoon = {
    constant: /%(?:\.[ny]|[\w-]+)/,
    comment: {
      pattern: /::.*/,
      greedy: true
    },
    'class-name': [
      {
        pattern: /@(?:[A-Za-z0-9-]*[A-Za-z0-9])?/
      },
      /\*/
    ],
    function: /(?:\+[-+] {2})?(?:[a-z](?:[a-z0-9-]*[a-z0-9])?)/,
    string: {
      pattern: /"[^"]*"|'[^']*'/,
      greedy: true
    },
    keyword:
      /\.[\^\+\*=\?]|![><:\.=\?!]|=[>|:,\.\-\^<+;/~\*\?]|\?[>|:\.\-\^<\+&~=@!]|\|[\$_%:\.\-\^~\*=@\?]|\+[|\$\+\*]|:[_\-\^\+~\*]|%[_:\.\-\^\+~\*=]|\^[|:\.\-\+&~\*=\?]|\$[|_%:<>\-\^&~@=\?]|;[:<\+;\/~\*=]|~[>|\$_%<\+\/&=\?!]|--|==/
  }
}
