# .feastignore

## Overview

`.feastignore` is a file that is placed at the root of the [Feature Repository](feature-repository.md). This file contains paths that should be ignored when running `feast apply`. An example `.feastignore` is shown below:

{% code title=".feastignore" %}
```text
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

`.feastignore` file is optional. If the file can not be found, every Python in the feature repo directory will be parsed by `feast apply`.

## Feast Ignore Patterns

| Pattern | Example matches | Explanation |
| :--- | :--- | :--- |
| venv | venv/foo.py venv/a/foo.py | You can specify a path to a specific directory. Everything in that directory will be ignored. |
| scripts/foo.py | scripts/foo.py | You can specify a path to a specific file. Only that file will be ignored. |
| scripts/\*.py | scripts/foo.py scripts/bar.py | You can specify asterisk \(\*\) anywhere in the expression. An asterisk matches zero or more characters, except "/". |
| scripts/\*\*/foo.py | scripts/foo.py scripts/a/foo.py scripts/a/b/foo.py | You can specify double asterisk \(\*\*\) anywhere in the expression. A double asterisk matches zero or more directories. |

