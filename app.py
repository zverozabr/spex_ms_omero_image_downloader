from multiprocessing import freeze_support
from spex_common.modules.logging import get_logger
from spex_common.config import load_config
from models.Worker import Worker, get_pool_size


if __name__ == '__main__':
    freeze_support()
    load_config()
    logger = get_logger('spex.ms-oid')
    logger.info('Starting')

    workers = []
    for index in range(get_pool_size('WORKERS_POOL')):
        worker = Worker(index)
        workers.append(worker)
        worker.start()

    try:
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        pass

    logger.info('Finished')
