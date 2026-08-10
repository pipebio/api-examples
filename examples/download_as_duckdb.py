import os
import shutil
import zipfile
from inspect import getsourcefile
from os.path import dirname

from pipebio.models.export_format import ExportFormat
from pipebio.pipebio_client import PipebioClient

document_id = os.environ['TARGET_DOCUMENT_ID']
pipebio_url = os.environ.get('PIPEBIO_URL', 'https://app.pipebio.com')

client = PipebioClient(url=pipebio_url)

# ExportJob returns a ZIP archive; download it, then extract the .db file.
# Open the extracted database with duckdb.connect(path) or ATTACH.
destination_dir = dirname(getsourcefile(lambda: 0))
zip_paths = client.export(
    document_id,
    ExportFormat.DUCKDB,
    destination_dir,
    destination_filename='document.duckdb.zip',
)
if not zip_paths:
    raise RuntimeError('Export did not return any download links')

extracted_paths = []
for zip_path in zip_paths:
    print(f'Export archive downloaded to {zip_path}')
    with zipfile.ZipFile(zip_path) as archive:
        db_members = [
            name for name in archive.namelist()
            if name.endswith('.db') and not name.endswith('/')
        ]
        if len(db_members) != 1:
            raise RuntimeError(
                f'Expected one .db file in export, got: {archive.namelist()}'
            )

        destination_path = os.path.join(destination_dir, 'document.db')
        with archive.open(db_members[0]) as src, open(destination_path, 'wb') as dst:
            shutil.copyfileobj(src, dst)
        extracted_paths.append(destination_path)

for path in extracted_paths:
    print(f'DuckDB export saved to {path}')
