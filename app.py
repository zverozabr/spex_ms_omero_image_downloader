import spex_common.modules.omero_blitz as omero_blitz
from multiprocessing import freeze_support
from os import cpu_count, getenv
from spex_common.modules.logging import get_logger
from spex_common.config import load_config
from models.Worker import Worker


def connect_to_omero():
    return omero_blitz.create('shared', 'qwerty123456')


if __name__ == '__main__':
    freeze_support()
    load_config()
    logger = get_logger('spex.ms-oid')
    logger.info('Starting')

    logger.info('connect to omero')
    session = connect_to_omero()

    workers = []
    for index in range(max(cpu_count(), getenv('WORKERS_POOL', 1))):
        worker = Worker(index)
        workers.append(worker)
        worker.start()

    try:
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        pass

    session.close(True)
    logger.info('Finished')
