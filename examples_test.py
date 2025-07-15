import os
import sys
import subprocess
import pytest
from pathlib import Path

from pipebio.pipebio_client import PipebioClient


def get_project_root():
    """Get the absolute path to the project root."""
    return Path(__file__).parent


def pytest_generate_tests(metafunc):
    """Generate test cases dynamically for each example file."""
    if "example_file" in metafunc.fixturenames:
        examples_dir = get_project_root() / "examples"
        example_files = sorted(examples_dir.glob("*.py"))
        
        # Skip test if no examples found
        if not example_files:
            pytest.skip("No example files found in the examples directory")
            
        metafunc.parametrize("example_file", example_files, ids=[f.name for f in example_files])


class TestExamples:
    """Integration tests for all example scripts."""

    @staticmethod
    def run_example(script_path):
        """Run an example script as a subprocess and check return code."""
        # Run the script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            check=False
        )

        # Print output for debugging if there's a failure
        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        # Check that the script ran successfully
        assert result.returncode == 0, f"Example {script_path.name} failed with exit code {result.returncode}"

        return result
        
    def test_example(self, example_file):
        """Test that the given example file runs successfully."""
        to_skip = ['import_from_aws.py']
        
        if example_file.name in to_skip:
            pytest.skip(f"Skipping {example_file.name}")
        
        print(f"Testing {example_file.name}", flush=True)
        TestExamples.run_example(example_file)
        
    def test_cleanup(self):
        """Clean up resources after all tests have run."""
        TestExamples.clean_up()

    @staticmethod
    def clean_up():
        client = PipebioClient(url='https://app.pipebio.com')
        shareable_id = os.environ['TARGET_SHAREABLE_ID']
        folder_id = os.environ['TARGET_FOLDER_ID']
        entities = client.shareables.list_entities(shareable_id)
        target_folder = client.entities.get(folder_id)
        entity_ids = list(
            int(e['id']) for e in entities if e['path'].startswith(target_folder['path']) and e['id'] != folder_id)
        if len(entity_ids):
            client.entities.delete(entity_ids)