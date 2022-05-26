import time
import spex_common.modules.omero_blitz as omero_blitz
from os import cpu_count, getenv
from multiprocessing import Process
from multiprocessing.pool import ThreadPool
from spex_common.modules.logging import get_logger
from spex_common.modules.aioredis import create_aioredis_client
from spex_common.models.RedisEvent import RedisEvent
from spex_common.models.OmeroImageFileManager import OmeroImageFileManager
from spex_common.services.OmeroBlitz import get_original_files_info, download_original_files, can_download


EVENT_TYPE = 'omero/download/image'
MIN_CHUNK_SIZE = 1024 * 1024 * 10


def get_pool_size(env_name) -> int:
    value = getenv(env_name, 'cpus')
    if value.lower() == 'cpus':
        value = cpu_count()

    return max(1, int(value))


def __downloader(args):
    manager, user, index, alive = args

    image = manager.get_image_id()

    logger = get_logger(f'spex.ms-oid.thread.{image}.{index + 1}')

    logger.debug(f'alive = {alive["is_alive"]}')
    if not alive['is_alive']:
        return

    chunk_size = manager.get_chunk_size()

    logger.debug(f'Get session for user: {user}')
    session = omero_blitz.get(user, False)
    if session is None:
        logger.info(f'No blitz session for user: {user}')
        return

    from_position = index * chunk_size
    to_position = (index + 1) * chunk_size
    buffer_size = 1024*1024
    step = 0

    try:
        chunk = manager.open_chunk(index)
        _, tiff_data, _ = download_original_files(
            session.get_gateway(),
            image,
            from_position,
            to_position,
            buffer_size
        )

        logger.debug(f'Starting loading chunk {index} of image {image}')
        try:
            for block in tiff_data:
                if not alive['is_alive']:
                    logger.debug(f'terminate because alive = {alive["is_alive"]}')
                    return
                chunk.write(block)

                if step % 10 == 0:
                    logger.debug(f'{int((buffer_size*step / to_position) * 100)}%'
                                 f' loaded ({buffer_size*step} of {chunk_size})'
                                 f' chunk {index} of image {image}')

                step += 1

            logger.debug(f'100% loaded chunk {index} of image {image}')
            manager.finish_chunk(index)
        finally:
            chunk.close()
    except Exception as e:
        logger.exception(f'catch exception: {e}')
    finally:
        session.close()


async def __executor(logger, event):
    data = event.data

    if data is None:
        logger.debug('property data is None')
        return False

    image_id = data['id']
    user = data['user']
    override = data['override']

    logger.info(f'Processing image {image_id}')

    if image_id is None:
        logger.debug('property image_id is None')
        return False

    if user is None:
        logger.debug('property user is None')
        return False

    manager = OmeroImageFileManager(image_id)

    if not manager.is_available():
        logger.info(f'image {image_id} is not available for download! Skipping!')
        return False

    if manager.is_locked():
        logger.info(f'image {image_id} is locked! Skipping!')
        return False

    try:
        manager.lock()

        logger.debug(f'Get session for user: {user}')
        session = omero_blitz.get(user, False)
        if session is None:
            logger.info(f'No blitz session for user: {user}')
            return False

        try:
            gateway = session.get_gateway()

            if not override and manager.exists():
                logger.info(f'image {image_id} exists! Skipping!')
                return False

            if not can_download(gateway, image_id):
                logger.info(f'image {image_id} cannot be downloaded')
                manager.make_not_available()
                return False

            files_info = get_original_files_info(gateway, image_id)

            logger.info(f'image {image_id} has files_info: {files_info}')

            image_size = files_info['size']

            logger.debug(f'image {image_id} has size: {image_size}')

            # pool_size = get_pool_size('WORKER_THREADS_POOL')
            manager.set_expected_size(image_size)
            # chunk_size = max(MIN_CHUNK_SIZE, math.ceil(image_size / pool_size))
            manager.set_chunk_size(MIN_CHUNK_SIZE)

            alive = {'is_alive': True}

            pool = ThreadPool(processes=get_pool_size('WORKER_THREADS_POOL'))
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

                return manager.exists()
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
    except Exception as e:
        logger.exception(f'catch exception: {e}')
    finally:
        manager.unlock()


def worker(name):
    logger = get_logger(name)
    redis_client = create_aioredis_client()

    @redis_client.event(EVENT_TYPE)
    async def listener(event: RedisEvent):
        if event is None or event.is_viewed:
            return
        logger.debug(f'catch event: {event}')
        event.set_is_viewed()
        try:
            result = await __executor(logger, event)
            if result:
                await redis_client.send(f'{EVENT_TYPE}/done', event.data)
        except KeyboardInterrupt as err:
            raise err
        except Exception as err:
            logger.exception(f'catch exception: {err}')
            # TODO maybe better to send error event
            await redis_client.send(f'{EVENT_TYPE}/done', event.data)

    try:
        logger.info('Starting')
        redis_client.run(5)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(f'catch exception: {e}')
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
