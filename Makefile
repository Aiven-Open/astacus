SHORT_VER = $(shell git describe --tags --abbrev=0 | cut -f1-)
LONG_VER = $(shell git describe --long 2>/dev/null || echo $(SHORT_VER)-0-unknown-g`git describe --always`)
PYTHON_SOURCE_DIRS = astacus/
PYTHON_TEST_DIRS = tests/
ALL_PYTHON_DIRS = $(PYTHON_SOURCE_DIRS) $(PYTHON_TEST_DIRS)

# protobuf handling
PROTODIR=astacus/proto
PROTOBUFS = $(wildcard $(PROTODIR)/*.proto)
GENERATED_PROTOBUFS = $(patsubst %.proto,%_pb2.py,$(PROTOBUFS))

GENERATED = astacus/version.py $(GENERATED_PROTOBUFS)

PYTHON = python3
DNF_INSTALL = sudo dnf install -y

ZOOKEEPER_VERSION = 3.8.3
ZOOKEEPER_HASH = e8abdad74fd1c76e8f4f41b25ed5a0c1ed3b59c8642281396a16d2cd5bf22ad4

default: $(GENERATED)

venv: default
	$(PYTHON) -m venv venv && source venv/bin/activate && pip install -U pip && pip install -r requirements.txt

clean:
	rm -rf rpm/ $(GENERATED)

.PHONY: build-dep-fedora
build-dep-fedora:
	sudo dnf -y install --best --allowerasing rpm-build
	sudo dnf -y install 'dnf-command(builddep)'
	sudo dnf -y builddep astacus.spec

.PHONY: build-dep-ubuntu
build-dep-ubuntu:
	sudo sh -c 'apt-get update && apt-get install -y git libsnappy-dev python3-pip python3-psycopg2 protobuf-compiler'

.PHONY: test-dep-ubuntu
test-dep-ubuntu:
	wget --no-verbose https://dlcdn.apache.org/zookeeper/zookeeper-$(ZOOKEEPER_VERSION)/apache-zookeeper-$(ZOOKEEPER_VERSION)-bin.tar.gz
	echo "$(ZOOKEEPER_HASH)  apache-zookeeper-$(ZOOKEEPER_VERSION)-bin.tar.gz" | sha256sum -c -
	tar vxf apache-zookeeper-$(ZOOKEEPER_VERSION)-bin.tar.gz --wildcards apache-zookeeper-$(ZOOKEEPER_VERSION)-bin/lib/*.jar
	sudo cp -r apache-zookeeper-$(ZOOKEEPER_VERSION)-bin/lib /usr/share/zookeeper

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
	rm -rf htmlcov
	python3 -m pytest --cov=./ \
		--cov-report=html \
		--cov-report term-missing \
		--cov-report xml:coverage.xml \
		-s -vvv -x tests/

.PHONY: test
test: lint copyright unittest

.PHONY: isort
isort:
	pre-commit run isort --all-files

.PHONY: yapf
yapf: black

.PHONY: black
black:
	pre-commit run black --all-files

.PHONY: reformat
reformat: isort black

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
	pip list --outdated


# For development purposes, run server with the default astacus conf
# and 'something' to be backed up
BACKUPROOT=/tmp/astacus/src
BACKUPSTORAGE=/tmp/astacus/backup
run-server:
	rm -rf /tmp/astacus
	mkdir -p $(BACKUPROOT)
	mkdir -p $(BACKUPSTORAGE)
	dd if=/dev/zero of=$(BACKUPROOT)/zeros bs=1000 count=1000
	dd if=/dev/urandom of=$(BACKUPROOT)/random bs=1000 count=1000
	echo foo > $(BACKUPROOT)/foo
	echo foo > $(BACKUPROOT)/foo2
	echo bar > $(BACKUPROOT)/bar
	astacus server -c examples/astacus-files-local.yaml

.PHONY: rpm
rpm: $(GENERATED) /usr/bin/rpmbuild /usr/lib/rpm/check-buildroot
	git archive --output=astacus-rpm-src.tar --prefix=astacus/ HEAD
	# add generated files to the tar, they're not in git repository
	tar -r -f astacus-rpm-src.tar --transform=s,astacus/,astacus/astacus/, $(GENERATED)
	rpmbuild -bb astacus.spec \
		--define '_topdir $(PWD)/rpm' \
		--define '_sourcedir $(CURDIR)' \
		--define 'major_version $(SHORT_VER)' \
		--define 'minor_version $(subst -,.,$(subst $(SHORT_VER)-,,$(LONG_VER)))'
	$(RM) astacus-rpm-src.tar

astacus/version.py: version_from_git.py
	$(PYTHON) $^ $@

%_pb2.py: %.proto
	protoc -I $(PROTODIR) $< --python_out=$(PROTODIR)
