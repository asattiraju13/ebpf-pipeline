NAME = .
PIPENV := $(shell command -v pipenv 2> /dev/null)

.PHONY: lint
lint:
	$(PIPENV) run mypy --strict $(NAME)
	$(PIPENV) run bandit -r $(NAME)
	$(PIPENV) run isort --profile=black --check-only ./$(NAME) --diff
	$(PIPENV) run black --check $(NAME) --diff
	$(PIPENV) run pylint --recursive=y $(NAME)

	
