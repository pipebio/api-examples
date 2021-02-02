import os
import pickle
from pathlib import Path
import tempfile
import requests
from requests import Session

from library.util import Util


class Authentication:
    url: str
    session: requests.Session

    disk_location = '{}/pipebio_session'.format(tempfile.gettempdir())

    def __init__(self, url: str, session=Session()):
        self.session = session
        self.url = url

    def login(self, email: str = None, password: str = None, token: str = None):
        # TODO Better if we check session expiration as well as just that it exists.
        if self.session_exists_on_disk():
            print('\n\nAlready logged in. Reading session from disk.')
            self.read_session_from_disk()
        else:
            print('\n\nNo session found on disk. Logging in via HTTP.')

            if email is None:
                print('No email specified. Logging in via ENV variables')
                self._login_from_env()
            else:
                self._login(email, password, token)
        return {'session': self.session, 'user': self.user}

    def _login_from_env(self):
        email = os.environ['PIPE_EMAIL'] if 'PIPE_EMAIL' in os.environ else None
        password = os.environ['PIPE_PASSWORD'] if 'PIPE_PASSWORD' in os.environ else None
        token = os.environ['PIPE_TOKEN'] if 'PIPE_TOKEN' in os.environ else None

        if email is None or password is None:
            print('Email, password and potentially 2FA token are required to login.')
            print('PIPE_EMAIL={}, PIPE_PASSWORD={}, PIPE_TOKEN={}'.format(email, password, token))
            quit()

        else:
            self._login(email, password, token)

    def _login(self, email: str, password: str, token: str = None):
        self.session.cookies.clear()
        self.session = Util.mount_standard_session(self.session)

        headers = {}

        body = {
            'email': email,
            'password': password,
        }

        if token:
            body['token'] = token

        response = self.session.post(
            url='{}/api/auth'.format(self.url),
            json=body,
            headers=headers,
        )

        Util.raise_detailed_error(response)

        self.user = response.json()
        # Persist so can quickly run again without needing to enter 2FA token.
        self.save_session_to_disk()

    def session_exists_on_disk(self):
        my_file = Path(self.disk_location)
        return my_file.exists()

    def save_session_to_disk(self):
        with open(self.disk_location, 'wb') as f:
            disk = {
                'cookies': self.session.cookies,
                'user': self.user,
            }
            pickle.dump(disk, f)

    def read_session_from_disk(self):
        with open(self.disk_location, 'rb') as f:
            disk_dict = pickle.load(f)
            self.session.cookies.update(disk_dict['cookies'])
            self.user = disk_dict['user']

    def clear_session_on_disk(self):
        os.remove(self.disk_location)
