# scrumptious-squad-de-jan-23-proj

## Initial set up & Checks

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

## Deploying into a new sandbox
1. Within terraform/vars.tf, ensure the email within the email variable is correct.
2. cd into terraform/create_secrets_bucket and run:
```sh
terraform init
```
```sh
terraform plan
```
```sh
terraform apply
```
3. cd out to the main terraform directory and run the same commands again. This will initialise the data structure and create a topic & subscription to allow Cloudwatch email alerts. To receive alerts, confirm the subscription from your email.