
class OutputLink:
    expiry_timestamp: str
    url: str

    def __init__(self, url: str, expiry_timestamp: str):
        self.url = url
        self.expiry_timestamp = expiry_timestamp

    def to_json(self):
        return {'url': self.url, 'expires': self.expiry_timestamp}