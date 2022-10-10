import inspect
import logging
import os
from Bio.Seq import translate

import requests
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from library.models.sequence_document_kind import SequenceDocumentKind

DEFAULT_TIMEOUT = 60  # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class Util:

    # Copied from the google library.
    @staticmethod
    def raise_detailed_error(request_object):
        try:

            if request_object.status_code not in [200, 201]:
                print(request_object.text)

            request_object.raise_for_status()
        except HTTPError as e:
            raise HTTPError(e, request_object.text)

    @staticmethod
    def mount_standard_session(session: requests.Session, retry_post=False):
        # Remove previously mounted sessions.
        session.close()
        logging.basicConfig(level=logging.INFO)
        # NOTE: We often use POST for "READ" operations. Can we retry on those specifically?
        methods = ['HEAD', 'GET', 'OPTIONS', 'TRACE', 'PUT', 'PATCH', 'DELETE']
        if retry_post:
            methods.append('POST')

        retries = Retry(total=5,
                        backoff_factor=0,
                        status_forcelist=[
                            100, 101, 102, 103, 104,
                            404, 408, 429,
                            500, 502, 503, 504
                        ],
                        connect=5,
                        read=5,
                        method_whitelist=methods
                        )
        # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
        session.mount('http://', TimeoutHTTPAdapter(max_retries=retries))
        session.mount('https://', TimeoutHTTPAdapter(max_retries=retries))
        return session

    @staticmethod
    def get_executed_file_location():
        # @see https://stackoverflow.com/a/44592299
        filename = inspect.getframeinfo(inspect.currentframe()).filename
        return os.path.dirname(os.path.abspath(filename))

    @staticmethod
    def get_sequence_kind(sequence: str) -> SequenceDocumentKind:
        try:
            not_an_alignment = sequence.replace('-', '')
            translate(not_an_alignment)
            return SequenceDocumentKind.DNA
        except:
            return SequenceDocumentKind.AA
