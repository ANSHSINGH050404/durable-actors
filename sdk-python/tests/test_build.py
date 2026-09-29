import json
import subprocess
import sys
import zipfile

import pytest


def test_hosted_build_installs_dependencies_before_import_and_bundles_resources(tmp_path):
    dependency = tmp_path / "build_dep-1.0-py3-none-any.whl"
    with zipfile.ZipFile(dependency, "w") as wheel:
        wheel.writestr("build_dep/__init__.py", "initial_count = 7\n")
        wheel.writestr(
            "build_dep-1.0.dist-info/METADATA",
            "Metadata-Version: 2.1\nName: build-dep\nVersion: 1.0\n",
        )
        wheel.writestr(
            "build_dep-1.0.dist-info/WHEEL",
            "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        wheel.writestr("build_dep-1.0.dist-info/RECORD", "")
    project = tmp_path / "project"
    project.mkdir()
    (project / "data").mkdir()
    (project / "data/__init__.py").write_text("")
    (project / "data/message.txt").write_text("hello")
    (project / "src").mkdir()
    (project / "src/actors.py").write_text("""from importlib.resources import files
from build_dep import initial_count
from durable_actors import Actor
class Counter(Actor):
    count: int = initial_count
    def read(self) -> str:
        return f"{self.count}:{files('data').joinpath('message.txt').read_text()}"
""")
    (project / "pyproject.toml").write_text(f"""[project]
name = "test-actors"
version = "0.1.0"
dependencies = ["build-dep @ {dependency.as_uri()}"]
[tool.durable-actors]
include = ["data/*.txt"]
""")
    output = tmp_path / "output"
    result = subprocess.run(
        [sys.executable, "-m", "durable_actors.build", str(project), "src/actors.py", str(output)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["actors"][0]["actorName"] == "Counter"
    loaded = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from pathlib import Path; from durable_actors.build import load_artifact; print(load_artifact(Path(sys.argv[1]))[0]().read())",
            str(output / "actors.pyz"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert loaded.returncode == 0, loaded.stderr
    assert loaded.stdout.strip() == "7:hello"


def test_contract_is_extracted_from_packaged_source_not_stale_bytecode(tmp_path):
    import os
    import py_compile

    source = tmp_path / "actors.py"
    text = """from typing import Literal
from durable_actors import Actor
class Version(Actor):
    def read(self) -> Literal["one"]:
        return "one"
"""
    source.write_text(text)
    before = source.stat()
    py_compile.compile(str(source), doraise=True)
    source.write_text(text.replace("one", "two"))
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "durable_actors.build",
            str(tmp_path),
            "actors.py",
            str(tmp_path / "dist"),
            "local",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    schema = json.loads(result.stdout)["actors"][0]["rpc"]["schema"]
    assert schema["definitions"]["Method_read_Result"]["const"] == "two"


@pytest.mark.parametrize("directory", ["venv", "tools/python-env"])
def test_build_excludes_virtual_environment_sources(tmp_path, directory):
    from durable_actors.build import write_artifact

    (tmp_path / "actors.py").write_text("from helper import value\n")
    (tmp_path / "helper.py").write_text("value = 1\n")
    root = tmp_path / directory
    environment = root / "lib/python3.13/site-packages/dependency"
    environment.mkdir(parents=True)
    if directory != "venv":
        (root / "pyvenv.cfg").write_text("include-system-site-packages = false\n")
    with (environment / "__init__.py").open("wb") as source:
        source.truncate(33 * 1024 * 1024)
    output = tmp_path / "dist"
    output.mkdir()
    write_artifact(tmp_path, output, "actors", {})
    with zipfile.ZipFile(output / "actors.pyz") as artifact:
        assert set(artifact.namelist()) == {"durable-actors.json", "actors.py", "helper.py"}


@pytest.mark.parametrize("conflict", [None, "sdk", "dependency"])
def test_requirements_preserve_the_runtime_sdk(tmp_path, conflict):
    from importlib.metadata import version

    typing_version = "999.0.0" if conflict == "dependency" else version("typing-extensions")
    typing_wheel = tmp_path / f"typing_extensions-{typing_version}-py3-none-any.whl"
    typing_metadata = f"typing_extensions-{typing_version}.dist-info"
    with zipfile.ZipFile(typing_wheel, "w") as wheel:
        wheel.writestr(
            "typing_extensions.py", "raise RuntimeError('bundled dependency shadows runtime')\n"
        )
        wheel.writestr(
            f"{typing_metadata}/METADATA",
            f"Metadata-Version: 2.1\nName: typing-extensions\nVersion: {typing_version}\n",
        )
        wheel.writestr(
            f"{typing_metadata}/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        wheel.writestr(
            f"{typing_metadata}/RECORD",
            f"typing_extensions.py,,\n{typing_metadata}/METADATA,,\n{typing_metadata}/WHEEL,,\n",
        )
    sdk_version = "999.0.0" if conflict == "sdk" else version("durable-actors")
    dependency = tmp_path / f"durable_actors-{sdk_version}-py3-none-any.whl"
    metadata = f"durable_actors-{sdk_version}.dist-info"
    with zipfile.ZipFile(dependency, "w") as wheel:
        files = {
            "durable_actors/__init__.py": "raise RuntimeError('bundled SDK shadows runtime')\n",
            f"{metadata}/METADATA": f"Metadata-Version: 2.1\nName: durable-actors\nVersion: {sdk_version}\nRequires-Dist: typing-extensions=={typing_version}\n",
            f"{metadata}/WHEEL": "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        }
        for name, content in files.items():
            wheel.writestr(name, content)
        wheel.writestr(f"{metadata}/RECORD", "".join(f"{name},,\n" for name in files))
    (tmp_path / "requirements.txt").write_text(
        f"durable-actors @ {dependency.as_uri()}\ntyping-extensions @ {typing_wheel.as_uri()}\n"
    )
    (tmp_path / "actors.py").write_text("""from durable_actors import Actor
class Counter(Actor):
    def read(self) -> int:
        return 7
""")
    output = tmp_path / "dist"
    result = subprocess.run(
        [sys.executable, "-m", "durable_actors.build", str(tmp_path), "actors.py", str(output)],
        capture_output=True,
        text=True,
    )
    if conflict:
        assert result.returncode != 0
        assert "ResolutionImpossible" in result.stderr
        return
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["actors"][0]["actorName"] == "Counter"
    loaded = subprocess.run(
        [
            sys.executable,
            "-c",
            "from importlib.metadata import version; import durable_actors; "
            f"assert version('durable-actors') == '{sdk_version}'; "
            f"assert version('typing-extensions') == '{typing_version}'; "
            "print(durable_actors.Actor.__name__)",
        ],
        cwd=output / "python",
        capture_output=True,
        text=True,
    )
    assert loaded.returncode == 0, loaded.stderr
    assert loaded.stdout.strip() == "Actor"
