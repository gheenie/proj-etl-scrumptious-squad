# scrumptious-squad-de-jan-23-proj

setting-up
check which pyenv version are using by using:

pyenv versions
make sure we have pyenv version will be 3.11.1, if the version is not found please run:

pyenv install 3.11.1
and set up globally

pyenv global 3.11.1
Remember the aws uses 3.9.7. when using the aws, please run:

pyenv local 3.9.7
make the version is installed in the pyenv version

create a virtual environment. To do this run the command

python -m venv venv
it should create a venv directory at the root of your project. Run the activate binaries using

Everytime when you create a branch just run the below two commands

source venv/bin/activate
To set up, please run:

make dev-setup
To check saftey and coverage, please run:

make run-checks
to test the test-file:

make unit-test
Don't forget to deactivate your virtual environment afterwards with:

deactivate
