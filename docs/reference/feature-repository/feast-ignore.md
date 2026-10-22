# .feastignore

## Overview

`.feastignore` is a file that is placed at the root of the [Feature Repository](./). This file contains paths that should be ignored when running `feast apply`. An example `.feastignore` is shown below:

{% code title=".feastignore" %}
```
# Ignore virtual environment
venv

# Ignore a specific Python file
scripts/foo.py

# Ignore all Python files directly under scripts directory
scripts/*.py

# Ignore all "foo.py" anywhere under scripts directory
scripts/**/foo.py
```
{% endcode %}

`.feastignore` file is optional. If the file can not be found, every Python file in the feature repo directory will be parsed by `feast apply`.

## Feast Ignore Patterns

| Pattern             | Example matches                                                 | Explanation                                                                                                              |
| ------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| venv                | <p>venv/foo.py<br>venv/a/foo.py</p>                             | You can specify a path to a specific directory. Everything in that directory will be ignored.                            |
| scripts/foo.py      | scripts/foo.py                                                  | You can specify a path to a specific file. Only that file will be ignored.                                               |
| scripts/\*.py       | <p>scripts/foo.py<br>scripts/bar.py</p>                         | You can specify an asterisk (\*) anywhere in the expression. An asterisk matches zero or more characters, except "/".    |
| scripts/\*\*/foo.py | <p>scripts/foo.py<br>scripts/a/foo.py<br>scripts/a/b/foo.py</p> | You can specify a double asterisk (\*\*) anywhere in the expression. A double asterisk matches zero or more directories. |
