# scrumptious-squad-de-jan-23-proj

## Setting up & Checks

### First time only:

Check which pyenv versions you have available:
```sh
pyenv versions
```
We should be using pyenv version 3.11.1, if the version is not found please install and set up as the local:
```sh
pyenv install 3.11.1
```
```sh
pyenv local 3.11.1
```

Remember the AWS uses 3.9.7. When using AWS, please ensure you have this version installed and run:
```sh
pyenv local 3.9.7
```

<br/>

The first time you open the repo in VSCode, you need to create a virtual environment. To do this run:
```sh
python -m venv venv
```
A `venv` directory should be created at the root of your project.

<br/>

### Every time:
To activate the environment:
```sh
source venv/bin/activate
```
To set up, please run:
```sh
make dev-setup
```

<br/>

### Sitatuational:
To check saftey and coverage, please run:
```sh
make run-checks
```
To test the test-file:
```sh
make unit-test
```

To deactivate your virtual environment:
```sh
deactivate
```
