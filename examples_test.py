import os
import sys
import subprocess
import pytest
from pathlib import Path

import os
from pipebio.pipebio_client import PipebioClient


def get_project_root():
    """Get the absolute path to the project root."""
    return Path(__file__).parent


class TestExamples:
    """Integration tests for all example scripts."""

    def run_example(self, script_path):
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

    def test_all_examples(self):

        """Test that all Python files in the examples directory run successfully."""
        examples_dir = get_project_root() / "examples"
        example_files = examples_dir.glob("*.py")

        # Sort the files to ensure consistent test order
        example_files = sorted(example_files)

        to_skip = ['import_from_aws.py']

        # Skip test if no examples found
        if not example_files:
            pytest.skip("No example files found in the examples directory")

        for example_file in example_files:
            if example_file.name in to_skip:
                print(f"Skipping {example_file.name}", flush=True)
            else:
                print(f"Testing {example_file.name}", flush=True)
                result = self.run_example(example_file)

        self.clean_up()

    def clean_up(self):
        client = PipebioClient(url='https://app.pipebio.com')
        shareable_id = os.environ['TARGET_SHAREABLE_ID']
        folder_id = os.environ['TARGET_FOLDER_ID']
        entities = client.shareables.list_entities(shareable_id)
        target_folder = client.entities.get(folder_id)
        entity_ids = list(
            int(e['id']) for e in entities if e['path'].startswith(target_folder['path']) and e['id'] != folder_id)
        if len(entity_ids):
            client.entities.delete(entity_ids)
