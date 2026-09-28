import subprocess
import sys

import pytest

from little_actors import Actor
from little_actors.codegen import generate_client
from little_actors.contract import public_contract


def test_sandbox_options_preserve_actor_types_and_publish_runtime_overrides(tmp_path):
    from little_actors import sandbox

    @sandbox(cpu=2, memory_mib=2048, idle_timeout_ms=60000, regions=["canada"])
    class Agent(Actor):
        count: int = 0

        def increment(self) -> int:
            self.count += 1
            return self.count

    contract = public_contract([Agent])
    assert contract["actors"][0]["sandbox"] == {
        "cpu": 2,
        "memoryMiB": 2048,
        "idleTimeoutMs": 60000,
        "regions": ["canada"],
    }
    assert Agent().increment() == 1
    generate_client(contract, tmp_path / "resource_client")
    source = tmp_path / "usage.py"
    source.write_text("""from little_actors import Actor, sandbox
from resource_client import actors

@sandbox(cpu=0.125, memory_mib=512, idle_timeout_ms=1000, regions=["canada"])
class Agent(Actor):
    def increment(self) -> int:
        return 1

def check() -> int:
    return Agent.get("one").increment() + actors.Agent.get("two").increment()
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    source.write_text(
        source.read_text() + '\nsandbox(memory_mib="huge")\nsandbox(regions=["moon"])\n'
    )
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1 and "2 errors" in result.stdout, result.stdout + result.stderr


@pytest.mark.parametrize(
    "options",
    [
        {"cpu": 0.099},
        {"cpu": 64.001},
        {"cpu": 0.1234},
        {"cpu": True},
        {"memory_mib": 127},
        {"memory_mib": 262145},
        {"memory_mib": 128.5},
        {"idle_timeout_ms": 0},
        {"idle_timeout_ms": 86400001},
        {"regions": []},
        {"regions": ["canada", "canada"]},
        {"regions": ["moon"]},
    ],
)
def test_invalid_resource_options_fail_before_publication(options):
    from little_actors import sandbox

    with pytest.raises(ValueError):
        sandbox(**options)


def test_unspecified_sandbox_defaults_are_inherited_and_repeated_decorators_are_rejected():
    from little_actors import sandbox

    class Default(Actor):
        pass

    assert "sandbox" not in public_contract([Default])["actors"][0]
    configured = sandbox(cpu=0.5)(Default)
    assert public_contract([configured])["actors"][0]["sandbox"] == {"cpu": 0.5}
    with pytest.raises(ValueError, match="repeated"):
        sandbox(memory_mib=512)(configured)
