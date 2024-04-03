import os
import yaml

import pytest

from dbt.tests.util import run_dbt_and_capture, write_file

from tests.functional.partial_parsing.fixtures import (
    model_color_sql,
)


class TestProfileChanges:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_color.sql": model_color_sql,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
            "user": os.getenv("POSTGRES_TEST_USER", "root"),
            "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
            "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
        }

    def test_change_password(self, project, dbt_profile_data):
        # Fist run not partial parsing
        _, stdout = run_dbt_and_capture(["parse"])
        assert "Unable to do partial parsing because saved manifest not found" in stdout

        _, stdout = run_dbt_and_capture(["parse"])
        assert "Unable to do partial parsing" not in stdout

        # write a different profile
        dbt_profile_data["test"]["outputs"]["default"]["dbname"] = "dbt2"
        write_file(yaml.safe_dump(dbt_profile_data), project.profiles_dir, "profiles.yml")
        _, stdout = run_dbt_and_capture(["parse"])
        assert "Unable to do partial parsing because profile has changed" in stdout

        # Change the password
        dbt_profile_data["test"]["outputs"]["default"]["pass"] = "another_password"
        write_file(yaml.safe_dump(dbt_profile_data), project.profiles_dir, "profiles.yml")
        _, stdout = run_dbt_and_capture(["parse"])
        assert "Unable to do partial parsing" not in stdout
