from pipebio.pipebio_client import PipebioClient

client = PipebioClient(url='https://app.pipebio.com')

# Set document id to get the status chart for.
# NOTE: This document must be annotated or you will not get a good result back.
document_id = 'ent_TozXnaXkMVhEjFm9'

result = client.session.get(f'sequences/charts/StatusPieChart?documentId={document_id}')

data = result.json()

# Below are some examples of how to interact with the data returned from the StatusPieChart endpoint.
correct_count = data['status']['CORRECT']
assert correct_count == 131

incorrect_count = data['status']['INCORRECT']
assert incorrect_count == 6

assert data['total'] == correct_count + incorrect_count