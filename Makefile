SHORT_VER = $(shell git describe --tags --abbrev=0 | cut -f1-)
LONG_VER = $(shell git describe --long 2>/dev/null || echo $(SHORT_VER)-0-unknown-g`git describe --always`)
PYTHON_SOURCE_DIRS = astacus/
PYTHON_TEST_DIRS = tests/
ALL_PYTHON_DIRS = $(PYTHON_SOURCE_DIRS) $(PYTHON_TEST_DIRS)
GENERATED =
PYTHON = python3
DNF_INSTALL = sudo dnf install -y

default: $(GENERATED)

clean:
	rm -rf rpm/

.PHONY: build-dep-fedora
build-dep-fedora: /usr/bin/rpmbuild
	sudo dnf -y builddep astacus.spec

.PHONY: pylint
pylint: $(GENERATED)
	pre-commit run pylint --all-files

.PHONY: flake8
flake8: $(GENERATED)
	pre-commit run flake8 --all-files

.PHONY: copyright
copyright:
	$(eval MISSING_COPYRIGHT := $(shell git ls-files "*.py" | grep -v __init__.py | xargs grep -EL "Copyright \(c\) 20.* Aiven|Aiven license OK"))
	@if [ "$(MISSING_COPYRIGHT)" != "" ]; then echo Missing Copyright statement in files: $(MISSING_COPYRIGHT) ; false; fi

.PHONY: unittest
unittest: $(GENERATED)
	python3 -m pytest -s -vvv tests/

.PHONY: test
test: lint copyright unittest

.PHONY: isort
isort:
	pre-commit run isort --all-files

.PHONY: yapf
yapf:
	pre-commit run yapf --all-files

.PHONY: reformat
reformat: isort yapf

.PHONY: pre-commit
pre-commit: $(GENERATED)
	pre-commit run --all-files

.PHONY: lint
lint: pre-commit
