# Ministry of Justice data engineering template repository

Use this template to [create a repository](https://github.com/moj-analytical-services/data-engineering-template/generate) with the default initial files for a Ministry of Justice data engineering Github repository. These include:

* the correct licence
* pre-commit githooks
* .gitignore file

Once you have created your repository, please:

* edit this readme file to document your project
* grant permissions to the appropriate MoJ teams
* set up branch protection

This template is based on the more general [Ministry of Justice template repo](https://github.com/ministryofjustice/template-repository). 

## Githooks
This repo comes with some githooks to make standard checks before you commit files to Github. The checks are: 
- if you're using git-crypt, run `git-crypt status` and check for unencrypted file warnings 
- run Black on Python files
- run Flake8 on Python files
- run yamllint on yaml files

If you want to use these, run this command from the repo's root directory: 

`git config core.hooksPath githooks`

By default hooks live in a .git subfolder that isn't version controlled. This repo instead keeps them in a folder called `githooks`, in a bash script called `pre-commit`. The command above tells your copy of the repo to look in this folder instead of the usual location. 

You can add these hooks to other repos by copying from this repo's githooks folder and pasting into the other repo's .git/hooks folder.

### Skipping the hooks
Once installed, the hooks run each time you commit. To skip them, add `--no-verify` to the end of your commit command. For exmaple, `git commit -m "Committing stuff" --no-verify`.

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
