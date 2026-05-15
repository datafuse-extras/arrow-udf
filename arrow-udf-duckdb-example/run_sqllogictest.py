#!/usr/bin/env python3

import sys
from pathlib import Path

import pytest
import sqllogic.test_sqllogic as sqllogic_runner


def main() -> int:
    runner_path = Path(sqllogic_runner.__file__).resolve()
    return pytest.main(
        ["--noconftest", "-p", "sqllogic.conftest", str(runner_path), *sys.argv[1:]],
    )


if __name__ == "__main__":
    raise SystemExit(main())
