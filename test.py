from spex_common.config import load_config
from spex_common.modules.aioredis import send_event
from spex_common.services.Timer import every


def emitter():
    send_event('omero/download/image', {
        'id': 16,
        'override': False,
        'user': 'shared'
    })


if __name__ == '__main__':
    load_config()
    every(20, emitter)
