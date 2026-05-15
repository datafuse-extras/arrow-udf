.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# TODO: these values are currently duplicated in lib.rs. There's a PR open in duckdb-rs that fixes this
EXTENSION_NAME=arrow_udf_duckdb_example
TARGET_DUCKDB_VERSION=v1.0.0

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

TEST_RUNNER=$(PYTHON_VENV_BIN) $(PROJ_DIR)run_sqllogictest.py
TEST_RUNNER_BASE=$(TEST_RUNNER) --duckdb-root-dir $(PROJ_DIR) --test-dir test/sql $(EXTRA_EXTENSIONS_PARAM)
TEST_RUNNER_DEBUG=$(TEST_RUNNER_BASE) --build-dir build/debug
TEST_RUNNER_RELEASE=$(TEST_RUNNER_BASE) --build-dir build/release

configure: venv platform extension_version
configure: install_test_runner

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

install_test_runner: venv
	$(PYTHON_VENV_BIN) -m pip install pytest

clean: clean_build clean_rust
clean_all: clean_configure clean
