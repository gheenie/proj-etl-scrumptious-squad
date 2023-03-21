# scrumptious-squad-de-jan-23-proj

### setting-up

check which pyenv version are using by using:
```sh
pyenv versions
```
make sure we have pyenv version will be 3.11.1, if the version is not found please run:
```sh
pyenv install 3.11.1
```
and set up globally
```sh
pyenv global 3.11.1
```

Remember the aws uses 3.9.7. when using the aws, please run:
```sh
pyenv local 3.9.7
```
make the version is installed in the pyenv version 

create a virtual environment. To do this run the command
```sh
python -m venv venv
```
it should create a `venv` directory at the root of your project. Run the _activate_ binaries using

Everytime when you create a branch just run the below two commands
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
