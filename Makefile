PROJECT_NAME = Data_Engineering_Final_project
PYTHON_INTERPRETER = python
PYTHONPATH := $(shell pwd)
VENV_DIR := venv

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV = . $(VENV_DIR)/bin/activate

.PHONY: create-environment requirements update-pip security-test run-flake unit-test check-coverage run-checks all

# Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> Checking python3 version..."
	@$(PYTHON_INTERPRETER) --version
	@echo ">>> Setting up VirtualEnv."
	@pip install -q virtualenv virtualenvwrapper
	virtualenv $(VENV_DIR) --python=$(PYTHON_INTERPRETER)

# Build the environment requirements
requirements: create-environment
	$(ACTIVATE_ENV) && pip install -r requirements.txt

update-pip:
	$(PYTHON_INTERPRETER) -m pip install --upgrade pip

# Run the security test (bandit + safety) only on the src and tests directories
security-test:
	$(ACTIVATE_ENV) && safety check -r requirements.txt
	$(ACTIVATE_ENV) && bandit -lll src/*.py tests/*.py

# Run the flake8 code check only on the src and tests directories
run-flake:
	$(ACTIVATE_ENV) && flake8 src tests

# Run the coverage check only on the src and tests directories
check-coverage:
	$(ACTIVATE_ENV) && PYTHONPATH=$(PYTHONPATH) coverage run --omit 'venv/*' -m pytest src tests && coverage report -m

# Run the unit tests only on the src and tests directories
unit-test:
	$(ACTIVATE_ENV) && PYTHONPATH=$(PYTHONPATH) python -m pytest -v src tests


# Run all checks
run-checks: security-test run-flake unit-test check-coverage

# Run Everything
all: requirements update-pip run-checks
