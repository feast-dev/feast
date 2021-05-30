# Documentation Generation

This `docgen/` directory contains source code and configuration used to generate documentation containing code snippets.
The reason `docgen/` exists is to ensure that code snippets are tested by pytest as part of our CI process. By extracting
the code snippets into a `yaml` file we are able to verify the code independently.

### Structure

`docgen/` contains sub-folders that map directly to specific gitbook pages. Each subfolder must contain the following
* A `document.jinja2` file which will be rendered using Jinja into a markdown page and upserted into our Gitbook docs
* A `config.yml` file containing the following fields
    * `gitbook_output_file` which is the path of the Gitbook markdown file that will be replaced.
    * `steps` which contains a list of steps that will be tested by Pytest as part of CI
        * `step` each step contains the following
            * `name` used to lookup the code snippet as part of `document.jinja2` using `get_code_block('my_step_name')`
            * `command` a CLI command that will be run (either a `command` or `python_script` should be specified)
            * `python_script` a path to a local Python script that will be run
* All Python scripts referenced by `python_script` fields in `steps` should be available in a sub-folder

### How does it work?

Running
```commandline
Python render.py
```
will generate a `build` folder in each of the `docgen` subfolders. This folder contains a rendered `document.md`, which 
will also be added to our Gitbook docs in `docs/`. The folder also contains a `test_script.sh` which will be run by
our [test_snippets.py](../sdk/python/tests/test_snippets.py) pytest during CI.


### How do I regenerate docs and tests?

Just run `make format` from the root of the Feast repository.

### How do I test the code snippets?

You can run pytest on [test_snippets.py](../sdk/python/tests/test_snippets.py)

### How do I add another documentation test?

Make a copy of `docgen/quickstart` and modify its contents. The rest should be automatic. Specifically
* Remember to treat `document.jinja2` as the source of truth for writing and `config.yaml` for the source of truth for
code.

### Can I still modify documentation from Gitbook?

Not for pages that `docgen` contains.

