# TextDiff
## JavaScript diff library with support for visual, HTML-formatted output

This repository contains the diff functionality of the [google-diff-match-patch library](http://code.google.com/p/google-diff-match-patch/) by Neil Fraser, turned into a node module which is suitable for `require`ing into projects.

## [Demo](http://neil.fraser.name/software/diff_match_patch/svn/trunk/demos/demo_diff.html)

## Example usage

```javascript
var Diff = require('text-diff');

var diff = new Diff(); // options may be passed to constructor; see below
var textDiff = diff.main('text1', 'text2'); // produces diff array
diff.prettyHtml(textDiff); // produces a formatted HTML string

```

### Initialization options

Arguments may be passed into the `Diff` constructor in the form of an object:

- `timeout`: Number of seconds to map a diff before giving up (0 for infinity).
- `editCost`: Cost of an empty edit operation in terms of edit characters.

Example initialization with arguments: `var diff = new Diff({ timeout: 2, editCost: 6 });`

## Documentation

The API documentation below has been modified from the [original API documentation](https://code.google.com/p/google-diff-match-patch/wiki/API).

### Initialization

The first step is to create a new diff object (see example above). This object contains various properties which set the behaviour of the algorithms, as well as the following methods/functions:

### main(text1, text2) => diffs

An array of differences is computed which describe the transformation of text1 into text2. Each difference is an array.  The first element specifies if it is an insertion (1), a deletion (-1) or an equality (0). The second element specifies the affected text.

```javascript
main("Good dog", "Bad dog") => [(-1, "Goo"), (1, "Ba"), (0, "d dog")]
```

Despite the large number of optimisations used in this function, diff can take a while to compute. The `timeout` setting is available to set how many seconds any diff's exploration phase may take (see "Initialization options" section above). The default value is 1.0. A value of 0 disables the timeout and lets diff run until completion. Should diff time out, the return value will still be a valid difference, though probably non-optimal.

### cleanupSemantic(diffs) => null

A diff of two unrelated texts can be filled with coincidental matches. For example, the diff of "mouse" and "sofas" is `[(-1, "m"), (1, "s"), (0, "o"), (-1, "u"), (1, "fa"), (0, "s"), (-1, "e")]`. While this is the optimum diff, it is difficult for humans to understand. Semantic cleanup rewrites the diff, expanding it into a more intelligible format. The above example would become: `[(-1, "mouse"), (1, "sofas")]`. If a diff is to be human-readable, it should be passed to cleanupSemantic.

### cleanupEfficiency(diffs) => null

This function is similar to `cleanupSemantic`, except that instead of optimising a diff to be human-readable, it optimises the diff to be efficient for machine processing. The results of both cleanup types are often the same.

The efficiency cleanup is based on the observation that a diff made up of large numbers of small diffs edits may take longer to process (in downstream applications) or take more capacity to store or transmit than a smaller number of larger diffs. The `diff.EditCost` property sets what the cost of handling a new edit is in terms of handling extra characters in an existing edit. The default value is 4, which means if expanding the length of a diff by three characters can eliminate one edit, then that optimisation will reduce the total costs.

### levenshtein(diffs) => int

Given a diff, measure its Levenshtein distance in terms of the number of inserted, deleted or substituted characters. The minimum distance is 0 which means equality, the maximum distance is the length of the longer string.

### prettyHtml(diffs) => html

Takes a diff array and returns a string of pretty HTML. Deletions are wrapped in `<del></del>` tags, and insertions are wrapped in `<ins></ins>` tags. Use CSS to apply styling to these tags.

## Tests

Tests have not been ported to this library fork, however tests are [available in the original library](https://github.com/liddiard/google-diff-match-patch/tree/master/javascript). If you would like to port tests over, you will need to do some function call renaming (viz. the `diff_` prefix has been removed from functions in the fork) and remove tests specific to the "patch" and "match" functionalities of the original library.
