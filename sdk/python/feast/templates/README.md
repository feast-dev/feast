# Feast Templates

Each folder in this module is a template that comes packaged with Feast.

* A template is installed with `feast init -t template_name`
* The template name provided during `init` maps directly to the folder name
* It is possible to provide a bootstrap.py script with a template. The script must provide a bootstrap() function. This
  script will automatically be executed and can be used to set up data or sources for the user.
* The feature_store.yaml will have its `project` name templated based on the project name provided by the user. The
  default project name should be `my_project`. If a different name is chosen then no templating will occur.