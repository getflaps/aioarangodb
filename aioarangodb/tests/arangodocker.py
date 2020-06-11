from pytest_docker_fixtures.containers._base import BaseImage
from pytest_docker_fixtures.images import settings
import requests


settings['arango'] = {
    'image': 'arangodb',
    'version': '3.6.4',
    'env': {
        'ARANGO_ROOT_PASSWORD': 'secret',
        'ARANGO_NO_AUTH': '1'
    }
}

class ArangoDB(BaseImage):
    name = 'arango'
    port = 8529

    def check(self):
        url = f'http://{self.host}:{self.get_port()}/'
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        return False


arango_image = ArangoDB()