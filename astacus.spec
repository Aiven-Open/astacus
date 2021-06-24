Name:           astacus
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/astacus
Summary:        Cluster database backup tool
License:        ASL 2.0
Source0:        astacus-rpm-src.tar
BuildArch:      noarch

# These are used when e.g. in podman, creating package from scratch
# using pre-commit + pip; psycopg2 is indirect dependency of pghoard
BuildRequires:  git
BuildRequires:  protobuf-compiler
BuildRequires:  python3-chardet
BuildRequires:  python3-devel
BuildRequires:  python3-pip
BuildRequires:  python3-psycopg2
BuildRequires:  python3-wheel

# These are used when actually running the package
Requires:       pghoard
Requires:       python3-fastapi
Requires:       python3-httpx
Requires:       python3-protobuf
Requires:       python3-pyyaml
Requires:       python3-sentry-sdk
Requires:       python3-tabulate
Requires:       python3-typing-extensions
Requires:       python3-uvicorn

%undefine _missing_build_ids_terminate_build

%{?python_disable_dependency_generator}

%description
Cluster database backup tool


%prep
%setup -q -n astacus


%install

# We don't want setup.cfg derived hard versions as packager of
# python3-* may actually do something interesting and (for example)
# rename tabulate package python3-tabulate (cough).
grep -B 9999 "SKIP-IN-RPM" setup.cfg > setup.cfg.rpm
mv setup.cfg.rpm setup.cfg

python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
rm -rf %{buildroot}%{python3_sitelib}/tests/
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/astacus


%files
%defattr(-,root,root,-)
%doc LICENSE README.md examples/*.yaml examples/*.json
%{_bindir}/astacus*
%{python3_sitelib}/*


%changelog
* Tue Apr 28 2020 Markus Stenberg <mstenber@aiven.io> - 0.0.1
- Initial RPM package (in astacus)
