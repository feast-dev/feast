# Feast CLI

Install the Feast CLI using pip:

```bash
pip install feast==0.9.*
```

Configure the CLI to connect to your Feast Core deployment:

```text
feast config set core_url your.feast.deployment
```

{% hint style="info" %}
By default, all configuration is stored in `~/.feast/config`
{% endhint %}

The CLI is a wrapper around the [Feast Python SDK](python-sdk.md):

```aspnet
$ feast

Usage: feast [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  config          View and edit Feast properties
  entities        Create and manage entities    
  feature-tables  Create and manage feature tables
  jobs            Create and manage jobs
  projects        Create and manage projects
  version         Displays version and connectivity information
```

