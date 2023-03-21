# scrumptious-squad-de-jan-23-proj

### Setting up

check which pyenv version are using:
```sh
pyenv versions
```
We should have pyenv version 3.11.1, if the version is not found please run:
```sh
pyenv install 3.11.1
```
and set up locally
```sh
pyenv local 3.11.1
```

Remember the aws uses 3.9.7. When using the aws, please run:
```sh
pyenv local 3.9.7
```
make the version is installed in the pyenv version.

The first time you open the repo in VSCode, you need to create a virtual environment. To do this run:
```sh
python -m venv venv
```
A `venv` directory should be created at the root of your project.

Everytime when you create a branch just run the below two commands:
To activate the environment:
```sh
source venv/bin/activate
```
To set up, please run:
```sh
make dev-setup
```





To check saftey and coverage, please run:
```sh
make run-checks
```
to test the test-file:
```sh
make unit-test
```

Don't forget to deactivate your virtual environment afterwards with:
```sh
deactivate
```
