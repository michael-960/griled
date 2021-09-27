from datetime import datetime
#from .grile import Grile

class TempFileHandler:
    def __init__(self, grile):
        self.keys = set()
        self.grile = grile

    def generate_key(self):
        key = str(datetime.now()).replace('-', '_').replace('.', '_').replace(' ', '.')
        self.keys.add(key)
        pass

    def get_filename(self, key):
        return f'{key}@{id(self)}.tmp'

    def delete(self, key):
        pass

    def read(self, key, is_json=False):
        return self.grile.read('tmp', self.get_filename(key))

    def write(self, key, content, is_json=False):
        pass

    def __del__(self):
        pass
