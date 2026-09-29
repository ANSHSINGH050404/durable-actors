from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import tomllib
import zipfile
from contextlib import redirect_stdout
from importlib.metadata import distribution, distributions, version
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from .actor import Actor
from .contract import Document, public_contract
from .guards import is_actor

EXCLUDED_DIRECTORIES = {"node_modules", "venv", "__pycache__", "dist", "target", "generated"}


def build_actor(project: Path, entrypoint: str, output: Path, *, local: bool = True) -> Document:
    project, output = project.resolve(), output.resolve()
    source = (project / entrypoint).resolve()
    if not source.is_relative_to(project) or source.suffix != ".py" or not source.is_file():
        raise ValueError("entrypoint must be a Python file inside the project")
    if source.is_relative_to(output):
        raise ValueError("output must not contain the actor source")
    output.mkdir(parents=True, exist_ok=True)
    settings = project_settings(project)
    if not local:
        install_dependencies(project, output, settings)
    module = ".".join(source.relative_to(project).with_suffix("").parts)
    write_artifact(project, output, module, settings)
    with redirect_stdout(sys.stderr):
        contract = public_contract(load_artifact(output / "actors.pyz"))
    if len(json.dumps(contract).encode()) > 4 * 1024 * 1024:
        raise ValueError("public actor contract exceeds 4 MiB")
    return contract


def project_settings(project: Path) -> Document:
    manifest = project / "pyproject.toml"
    return tomllib.loads(manifest.read_text()) if manifest.is_file() else {}


def install_dependencies(project: Path, output: Path, settings: Document) -> None:
    dependencies = settings.get("project", {}).get("dependencies", [])
    requirements = project / "requirements.txt"
    if (
        not requirements.is_file()
        and settings.get("project", {}).get("dynamic")
        and "dependencies" in settings["project"]["dynamic"]
    ):
        raise ValueError("declare deployment dependencies explicitly or provide requirements.txt")
    arguments = (
        ["-r", str(requirements)] if requirements.is_file() else runtime_requirements(dependencies)
    )
    versions = runtime_versions()
    if arguments:
        install_requirements(project, output / "python", arguments, versions)
    remove_bundled_runtime(output / "python", versions)
    sys.path.insert(0, str(output / "python"))


def runtime_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    visited: set[tuple[str, tuple[str, ...]]] = set()
    pending = [Requirement("durable-actors")]
    while pending:
        requirement = pending.pop()
        name = canonicalize_name(requirement.name)
        extras = tuple(sorted(requirement.extras))
        if (name, extras) in visited:
            continue
        visited.add((name, extras))
        installed = distribution(name)
        versions[name] = installed.version
        for text in installed.requires or []:
            dependency = Requirement(text)
            if dependency.marker is None or any(
                dependency.marker.evaluate({"extra": extra}) for extra in ("", *extras)
            ):
                pending.append(dependency)
    return versions


def install_requirements(
    project: Path, directory: Path, arguments: list[str], versions: dict[str, str]
) -> None:
    with TemporaryDirectory(prefix="durable-actors-constraints-") as temporary:
        constraints = Path(temporary) / "runtime.txt"
        constraints.write_text("".join(f"{name}=={value}\n" for name, value in versions.items()))
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-compile",
                "--target",
                str(directory),
                "--constraint",
                str(constraints),
                *arguments,
            ],
            cwd=project,
            check=True,
            stdout=sys.stderr,
        )


def remove_bundled_runtime(directory: Path, versions: dict[str, str]) -> None:
    for installed in distributions(path=[str(directory)]):
        name = canonicalize_name(installed.metadata["Name"])
        if name not in versions:
            continue
        if installed.version != versions[name]:
            raise ValueError(f"{name} version must match the build runtime")
        if installed.files is None:
            raise ValueError(f"installed {name} has no file manifest")
        for file in installed.files:
            remove_bundled_file(directory, directory / file)


def remove_bundled_file(directory: Path, file: Path) -> None:
    file = file.resolve()
    if not file.is_relative_to(directory) or not file.is_file():
        return
    file.unlink()
    for parent in file.parents:
        if parent == directory:
            break
        try:
            parent.rmdir()
        except OSError:
            break


def runtime_requirements(dependencies: list[str]) -> list[str]:
    result: list[str] = []
    for dependency in dependencies:
        requirement = Requirement(dependency)
        if canonicalize_name(requirement.name) == "durable-actors":
            if version("durable-actors") not in requirement.specifier:
                raise ValueError("actor SDK version must match the build runtime")
        else:
            result.append(dependency)
    return result


def write_artifact(project: Path, output: Path, module: str, settings: Document) -> None:
    paths = source_paths(project, output)
    for pattern in settings.get("tool", {}).get("durable-actors", {}).get("include", []):
        for path in project.glob(pattern):
            if path.is_file():
                if (
                    path.is_symlink()
                    or not path.resolve().is_relative_to(project)
                    or path.is_relative_to(output)
                ):
                    raise ValueError("included resources must stay inside the project")
                paths.add(path)
    if sum(path.stat().st_size for path in paths) > 32 * 1024 * 1024:
        raise ValueError("actor source artifact exceeds 32 MiB")
    artifact = output / "actors.pyz"
    with zipfile.ZipFile(artifact, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("durable-actors.json", json.dumps({"version": 1, "module": module}))
        directories: set[str] = set()
        for path in sorted(paths):
            relative = path.relative_to(project)
            if str(relative) == "durable-actors.json":
                raise ValueError("durable-actors.json is reserved for the artifact manifest")
            archive.write(path, relative.as_posix())
            directories.update(
                parent.as_posix() + "/" for parent in relative.parents if parent != Path(".")
            )
        for directory in sorted(directories):
            archive.writestr(directory, "")


def source_paths(project: Path, output: Path) -> set[Path]:
    paths: set[Path] = set()
    for directory, directories, files in os.walk(project):
        if "pyvenv.cfg" in files:
            directories.clear()
            continue
        directories[:] = sorted(
            name
            for name in directories
            if not name.startswith(".")
            and name not in EXCLUDED_DIRECTORIES
            and not (Path(directory) / name).is_symlink()
            and (Path(directory) / name).resolve() != output
        )
        paths.update(
            Path(directory) / name
            for name in files
            if name.endswith(".py") and not (Path(directory) / name).is_symlink()
        )
    return paths


def load_artifact(path: Path) -> list[type[Actor[Any, Any, Any, Any]]]:
    with zipfile.ZipFile(path) as archive:
        manifest = json.loads(archive.read("durable-actors.json"))
    if manifest.get("version") != 1:
        raise ValueError("unsupported Python actor artifact")
    sys.path.insert(0, str(path.parent / "python"))
    return load_module(path, manifest["module"])


def load_module(root: Path, module: str) -> list[type[Actor[Any, Any, Any, Any]]]:
    sys.path[:0] = [str(root), str(root / "src")]
    importlib.invalidate_caches()
    loaded = importlib.import_module(module)
    exported = getattr(loaded, "__all__", vars(loaded))
    return [
        value
        for name, value in vars(loaded).items()
        if name in exported and not name.startswith("_") and is_actor(value)
    ]


def main() -> None:
    project, entrypoint, output, *mode = sys.argv[1:]
    contract = build_actor(Path(project), entrypoint, Path(output), local=mode == ["local"])
    print(json.dumps(contract, separators=(",", ":")))


if __name__ == "__main__":
    main()
