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
build-dep-fedora:
	sudo dnf -y install --best --allowerasing \
		golang \
		postgresql-server \
		python3-botocore python3-cryptography python3-paramiko python3-dateutil python3-devel \
		python3-flake8 python3-psycopg2 python3-pylint python3-pytest \
		python3-pytest-cov python3-requests python3-snappy \
		python3-azure-storage \
		rpm-build
	sudo dnf -y install 'dnf-command(builddep)'
	sudo dnf -y builddep astacus.spec

.PHONY: build-dep-ubuntu
build-dep-ubuntu:
	sudo apt-get install -y git libsnappy-dev python3-pip python3-psycopg2

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

# Utility targets to ensure that build-dep-X are up to date. These are
# NOT optimized for normal development.
PODMAN_RUN = podman run --rm --security-opt label=disable -v `pwd`:/src
# ^ without label=disable, modern selinux won't be happy

.PHONY: podman-test
podman-test: podman-test-fedora podman-test-ubuntu


.PHONY: podman-test-fedora
podman-test-fedora:
	podman build -t astacus-fedora -f Dockerfile.fedora
	$(PODMAN_RUN) astacus-fedora

.PHONY: podman-test-ubuntu
podman-test-ubuntu:
	podman build -t astacus-ubuntu -f Dockerfile.ubuntu
	$(PODMAN_RUN) astacus-ubuntu

.PHONY: pip-outdated
pip-outdated:
	pip-outdated setup.cfg requirements.testing.txt
