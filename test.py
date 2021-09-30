from spex_common.config import load_config
from spex_common.modules.aioredis import send_event
from spex_common.services.Timer import every


counter = 1


def emitter():
    global counter
    send_event('omero/download/image', {
        'id': counter % 20,
        'override': False,
        'user': 'shared'
    })
    counter += 1


if __name__ == '__main__':
    load_config()
    every(20, emitter)
