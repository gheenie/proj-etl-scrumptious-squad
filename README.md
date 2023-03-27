# scrumptious-squad-de-jan-23-proj

## When setting up the project for the first time:

<br />

1. Check which pyenv versions you have available:
```sh
pyenv versions
```
We should be using pyenv version 3.11.1 as stated in `.python_version`, if 
the version is not found please install and set up as the local:
```sh
pyenv install 3.11.1
```
```sh
pyenv local 3.11.1
```

<br />

2. Install Make.

On Linux, run:
```
sudo apt install make -y
```

<br />

3. Install dependencies, check code safety and coverage, and run tests with:
```sh
make all
```

<br />

4. Deploy into a new sandbox
    1. Create your AWS instance
    2. Set up your AWS credentials locally by running 
    ```
    aws configure
    ```
    3. If your instance's region is not us-east-1, in `terraform/`, change the region in `backend.tf` and `provider.tf` to match yours
    4. In `terraform/create_secrets_bucket/s3.tf` rename `secrets_bucket` to a unique name
    5. In `terraform/backend.tf` rename `backend s3`'s bucket to match the previous step
    6. In `terraform/vars.tf`, ensure the email within the email variable is correct
    7. From the PROJECT ROOT, run:
    ```sh
    cd terraform/create_secrets_bucket
    terraform init && terraform apply -auto-approve
    cd ..
    terraform init && terraform apply -auto-approve
    ```

<br />

5. Confirm the SNS subscription in the email used in step 4e

<br />

6. In your GitHub repo's secrets, make two new secrets. This is to enable CI/CD via GitHub Actions
    1. Name the first AWS_ACCESS_KEY, and in its contents copy-paste your AWS access key.
    2. Name the second AWS_SECRET_KEY, and in its contents copy-paste your AWS secret key.

<br />

## When working with the repo:

<br />

- Every time you come back to work on the repo (e.g. you've closed your IDE), activate your virtual environment (set up in step 3) 
and set the correct path for importing packages with:
```
source venv/bin/activate
export PYTHONPATH=$(pwd)
```

- When you've finished working, deactivate your virtual environment:
```sh
deactivate
```

<br />

## When your AWS instance is destroyed and you're remaking a new one:

<br />

- Follow step 4 again, but between 4e and 4f, in both `terraform/create_secrets_bucket` and `terraform/`,
delete any folders and files beginning with `.terraform` and any `terraform.tfstate` files.
- Retry this if step 4f fails.
