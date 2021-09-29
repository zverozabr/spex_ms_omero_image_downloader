import time
import spex_common.modules.omero_blitz as omero_blitz
from os import cpu_count, getenv
from multiprocessing import Process
from multiprocessing.pool import ThreadPool
from spex_common.modules.logging import get_logger
from spex_common.modules.aioredis import create_aioredis_client
from spex_common.models.OmeroImageFileManager import OmeroImageFileManager
from spex_common.services.OmeroBlitz import get_image_size, export_ome_tiff


EVENT_TYPE = 'omero/download/image'
FILE_CHUNK_SIZE = 1024*1024*10


def __downloader(args):
    manager, user, index, alive = args

    image = manager.get_image_id()

    logger = get_logger(f'spex.ms-oid.thread.{image}.{index + 1}')

    logger.debug(f'alive = {alive["is_alive"]}')
    if not alive['is_alive']:
        return

    session = omero_blitz.get(user, False)
    from_position = index * FILE_CHUNK_SIZE
    to_position = (index + 1) * FILE_CHUNK_SIZE

    try:
        chunk = manager.open_chunk(index)
        _, tiff_data = export_ome_tiff(
            session.get_gateway(),
            image,
            from_position,
            to_position,
            buffer_size=1024*512
        )

        try:
            for block in tiff_data:
                if not alive['is_alive']:
                    logger.debug(f'terminate because alive = {alive["is_alive"]}')
                    return
                chunk.write(block)
            manager.finish_chunk(index)
        finally:
            chunk.close()
    except Exception:
        logger.exception()
    finally:
        session.close()


async def __executor(logger, event):
    data = event.data

    if data is None:
        logger.debug('property data is None')
        return

    image_id = data['id']
    user = data['user']
    override = data['override']

    if image_id is None:
        logger.debug('property image_id is None')
        return

    if user is None:
        logger.debug('property user is None')
        return

    manager = OmeroImageFileManager(
        image_id,
        chunk_size=FILE_CHUNK_SIZE
    )

    if manager.is_locked():
        logger.debug(f'image {image_id} is locked! Skipping!')
        return

    try:
        manager.lock()

        logger.debug(f'get session')
        session = omero_blitz.get(user, False)
        if session is None:
            logger.info("No blitz session")
            return

        try:
            gateway = session.get_gateway()

            image_size = get_image_size(gateway, image_id)

            logger.debug(f'image {image_id} has size: {image_size}')

            manager.set_expected_size(image_size)

            if not override and manager.exists():
                logger.debug(f'image {image_id} exists! Skipping!')
                return

            alive = {'is_alive': True}

            pool = ThreadPool(processes=max(cpu_count(), getenv('WORKER_THREADS_POOL', 1)))
            try:
                chunks = manager.get_unfinished_chunks()
                while len(chunks) > 0:
                    logger.debug(f'unfinished chunks: {chunks}')

                    chunks = [(manager, user, index, alive) for index in chunks]

                    task = pool.map_async(__downloader, chunks)

                    while not task.ready():
                        time.sleep(0.5)

                    chunks = manager.get_unfinished_chunks()

                manager.merge_chunks()
            except KeyboardInterrupt:
                pool.terminate()
                raise
            finally:
                alive['is_alive'] = False
                pool.close()
                pool.join()
        finally:
            logger.debug(f'leave session')
            session.close()
    except KeyboardInterrupt:
        raise
    except Exception:
        logger.exception()
    finally:
        manager.unlock()


def worker(name):
    logger = get_logger(name)
    redis_client = create_aioredis_client()

    @redis_client.event(EVENT_TYPE)
    async def listener(event):
        logger.debug(f'catch event: {event}')
        await __executor(logger, event)

    try:
        logger.info('Starting')
        redis_client.run()
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception()
    finally:
        logger.info('Closing')
        redis_client.close()


class Worker(Process):
    def __init__(self, index=0):
        super().__init__(
            name=f'Spex.OID.Worker.{index + 1}',
            target=worker,
            args=(f'spex.ms-oid.worker.{index + 1}', ),
            daemon=True
        )
