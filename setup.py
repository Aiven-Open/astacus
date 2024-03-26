"""
astacus - setup

Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
import setuptools


def _run():
    from astacus.version import __version__ as version_for_setup_py  # pylint: disable=import-outside-toplevel

    setuptools.setup(
        version=version_for_setup_py,
        packages=setuptools.find_packages(exclude=["tests"]),
        extras_require={
            "cassandra": ["cassandra-driver==3.20.2"],
        },
        dependency_links=[],
        package_data={},
        entry_points={
            "console_scripts": [
                "astacus = astacus.main:main",
            ],
        },
        author="Aiven",
        author_email="support@aiven.io",
        license="Apache 2.0",
        platforms=["POSIX", "MacOS"],
        description="Astacus",
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "Intended Audience :: Information Technology",
            "Intended Audience :: System Administrators",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python :: 3.11",
            "Topic :: Database :: Database Engines/Servers",
            "Topic :: Software Development :: Libraries",
        ],
    )


if __name__ == "__main__":
    _run()
