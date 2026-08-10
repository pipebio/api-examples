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

# ExportJob returns a ZIP archive; download it, then extract the Parquet files.
destination_dir = dirname(getsourcefile(lambda: 0))
zip_paths = client.export(
    document_id,
    ExportFormat.PARQUET,
    destination_dir,
    destination_filename='document.parquet.zip',
)
if not zip_paths:
    raise RuntimeError('Export did not return any download links')

extracted_paths = []
for zip_path in zip_paths:
    print(f'Export archive downloaded to {zip_path}')
    with zipfile.ZipFile(zip_path) as archive:
        parquet_members = [
            name for name in archive.namelist()
            if name.endswith('.parquet') and not name.endswith('/')
        ]
        if not parquet_members:
            raise RuntimeError(
                f'Expected .parquet files in export, got: {archive.namelist()}'
            )

        for index, member in enumerate(parquet_members):
            if len(parquet_members) == 1:
                destination_path = os.path.join(destination_dir, 'document.parquet')
            else:
                destination_path = os.path.join(
                    destination_dir, f'document-{index}.parquet'
                )
            with archive.open(member) as src, open(destination_path, 'wb') as dst:
                shutil.copyfileobj(src, dst)
            extracted_paths.append(destination_path)

for path in extracted_paths:
    print(f'Parquet export saved to {path}')
