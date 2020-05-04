Name:           astacus
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/astacus
Summary:        Cluster database backup tool
License:        ASL 2.0
Source0:        astacus-rpm-src.tar
BuildArch:      noarch
BuildRequires:  python3-devel
BuildRequires:  python3-flake8
BuildRequires:  python3-isort
BuildRequires:  python3-pylint
BuildRequires:  python3-tox
BuildRequires:  python3-yapf
BuildRequires:  pre-commit
Requires:       systemd

%undefine _missing_build_ids_terminate_build

%description
Cluster database backup tool


%prep
%setup -q -n astacus


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
rm -rf %{buildroot}%{python3_sitelib}/tests/
%{__install} -Dm0644 astacus.unit %{buildroot}%{_unitdir}/astacus.service
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/astacus


%files
%defattr(-,root,root,-)
%doc LICENSE README.md astacus.config.json
%{_bindir}/astacus*
%{_unitdir}/astacus.service
%{python3_sitelib}/*


%changelog
* Tue Apr 28 2019 Markus Stenberg <mstenber@aiven.io> - 0.0.1
- Initial RPM package (in astacus)
