"""
version

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from packaging import version as packaging_version

import importlib.util
import os
import subprocess

ASTACUS_VERSION_FILE = os.path.join(os.path.dirname(__file__), "astacus", "version.py")


def save_version(*, new_ver, old_ver, version_file):
    "Save new version file, if old_ver != new_ver"
    if not new_ver:
        return False
    packaging_version.parse(new_ver)  # just a validation step, we don't actually care about the result
    if not old_ver or new_ver != old_ver:
        with open(version_file, "w") as file_handle:
            file_handle.write(f'"""{__doc__}"""\n__version__ = "{new_ver}"\n')
    return True


def update_project_version_from_git(version_file):
    "Update the version_file, and return the version number stored in the file"
    project_root_directory = os.path.dirname(os.path.realpath(__file__))
    version_file_full_path = os.path.join(project_root_directory, version_file)
    module_spec = importlib.util.spec_from_file_location("verfile", version_file_full_path)
    module = importlib.util.module_from_spec(module_spec)
    file_ver = getattr(module, "__version__", None)

    os.chdir(os.path.dirname(__file__) or ".")
    try:
        git_out = subprocess.check_output(
            ["git", "--git-dir", os.path.join(project_root_directory, ".git"), "describe", "--always"],
            stderr=getattr(subprocess, "DEVNULL", None),
        )
    except (OSError, subprocess.CalledProcessError):
        pass
    else:
        git_ver = git_out.splitlines()[0].strip().decode("utf-8")
        if "." not in git_ver:
            git_ver = f"0.0.1-0-unknown{git_ver}"
        version, count_since_release, git_hash = git_ver.split("-")
        # this version will never be picked as a more recent version than the release version
        # which is what we probably want to prevent silly mistakes.
        new_version = f"{version}.dev{count_since_release}+{git_hash}"
        if save_version(new_ver=new_version, old_ver=file_ver, version_file=version_file):
            return git_ver

    if not file_ver:
        raise ValueError(f"version not available from git or from file {version_file!r}")

    return file_ver


if __name__ == "__main__":
    import sys

    if sys.argv[1] == "from-git":
        update_project_version_from_git(ASTACUS_VERSION_FILE)
    elif sys.argv[1] == "set-version":
        save_version(new_ver=sys.argv[2], old_ver=None, version_file=ASTACUS_VERSION_FILE)
    else:
        raise ValueError(f"Unknown command {sys.argv[1]!r}")
