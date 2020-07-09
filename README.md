# fuzzyfinder


## Formatting and linting configs
Config changes for flake8 go in .flake8. Our standard settings include:
- max line length to 88 to match team's preference (and Black default)
- ignore rule E203 which doesn't quite match PEP8 on spacing around colons (and conflicts with Black)
- ignore some folders like venv and .github

Config changes for Black should go in `pyproject.toml`. Our standard settings make no changes from default.

Config changes for yamllint should go in `.yamllint`.

Our standard settings use the default for both of these, so at the moment those configs make no changes.  

## Licence
[MIT Licence](LICENCE.md)
