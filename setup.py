"""
astacus - setup

Copyright (c) 2020 Aiven Ltd
See LICENSE for details
"""
import os
import setuptools


def _run():
    readme_path = os.path.join(os.path.dirname(__file__), "README.md")
    with open(readme_path, "r") as file_handle:
        readme_text = file_handle.read()

    try:
        import version  # pylint: disable=import-outside-toplevel
        version_for_setup_py = version.update_project_version("astacus/version.py")
        version_for_setup_py = ".dev".join(version_for_setup_py.split("-", 2)[:2])
    except ImportError:
        version_for_setup_py = "0.0.1"  # tox

    setuptools.setup(
        name="astacus",
        version=version_for_setup_py,
        zip_safe=False,
        packages=setuptools.find_packages(exclude=["test"]),
        install_requires=[
            "PyYAML==5.3.1",
            "fastapi==0.54.1",
            "httpx==0.12.1",
            "uvicorn==0.11.5",
            "sentry-sdk==0.14.4",
            "pghoard==2.1.0",
        ],
        extras_require={},
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
        long_description=readme_text,
        url="https://github.com/aiven/astacus/",
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "Intended Audience :: Information Technology",
            "Intended Audience :: System Administrators",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Topic :: Database :: Database Engines/Servers",
            "Topic :: Software Development :: Libraries",
        ],
    )


if __name__ == '__main__':
    _run()
