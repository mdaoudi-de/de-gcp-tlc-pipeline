.PHONY: venv install lint format test run

VENV_PYTHON=.\.venv\Scripts\python.exe

venv:
	python -m venv .venv

install:
	$(VENV_PYTHON) -m pip install -U pip
	$(VENV_PYTHON) -m pip install -e .
	$(VENV_PYTHON) -m pip install ruff black pytest

lint:
	$(VENV_PYTHON) -m ruff check .

format:
	$(VENV_PYTHON) -m black .
	$(VENV_PYTHON) -m ruff check . --fix

test:
	$(VENV_PYTHON) -m pytest

run:
	@echo "No runnable entrypoint yet. Step 2 will add ingestion scripts."
