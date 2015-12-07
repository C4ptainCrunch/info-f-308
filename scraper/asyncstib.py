import aiohttp
import asyncio
from xml.etree import ElementTree
from datetime import datetime
import time
import random
import logging

from stib.stib import Network
from models import Heading, db
import peewee_async as pa

PERIOD = 20
CONCURRENCY = 10
HEADERS = {'user-agent': "Python/3.5 aiohttp/0.19.0 - nimarcha@ulb.ac.be"}

logger = logging.getLogger('asyncstib')

class StibApiError(Exception):
    pass


def catcher(fn):
    async def inner(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except StibApiError as e:
            logger.warning("Stib error", exc_info=e)
        except Exception as e:
            logger.error("Coroutine %s(%s,%s) raised %s", fn, args[1], args[2], e, exc_info=e)
            return None
    return inner


async def route_data(line, way):
    URL = 'http://m.stib.be/api/getitinerary.php?line={}&iti={}'

    if way not in (1, 2):
        raise ValueError("way must be an integer of value 1 or 2")
    line = str(line)
    if len(line) > 3:
        raise ValueError("line must not have more than 3 chars")

    url = URL.format(line, way)

    async with aiohttp.get(url, headers=HEADERS) as response:
        if response.status != 200:
            raise StibApiError("HTTP Error :", response.status)
        return await response.read()


async def route_status(line, way, timeout=10):
    try:
        xml = await asyncio.wait_for(route_data(line, way), timeout)
    except asyncio.TimeoutError as e:
        raise StibApiError('Timeout') from e

    try:
        tree = ElementTree.fromstring(xml)
    except ElementTree.ParseError as e:
        raise StibApiError("Invalid XML") from e

    if tree.tag != "stops":
        raise StibApiError("Root tag shoud be 'stops' but is ", tree.tag)

    if tree.attrib['line'] != str(line) or tree.attrib['iti'] != str(way):
        raise StibApiError(
            "Response is not from queried line : we asked for line", line,
            "way", way, "and got line", tree.attrib['line'],
            "way", tree.attrib['iti']
        )

    stops = [stop for stop in tree if stop.tag == 'stop']
    output = []

    for stop in stops:
        has_vehicle = False
        for elem in stop:
            if elem.tag == 'present' and elem.text == 'TRUE':
                has_vehicle = True
                break
        output.append(has_vehicle)

    vehicule_count = output.count(True)
    if vehicule_count > (2 * len(output) / 3):
        raise StibApiError(
            "There is too much vehiclues on the line :",
            vehicule_count, "for",
            len(output), "stops."
        )
    if vehicule_count > (len(output) / 2):
        logger.info("Line %s,%s has %i vehicules for %i stops", line, way, vehicule_count, len(output))

    return output


@catcher
async def save_route(line, way, semaphore):
    # limit the number of coroutines
    # in this block
    async with semaphore:
        route = await route_status(line, way)

    logger.debug('Store line %s,%s', line, way)
    await pa.create_object(
        Heading, line=line,
        way=way, stops=route,
        timestamp=datetime.now()
    )



async def route_loop(sleep, line, way, semaphore):
    # distribute evenly the load
    await asyncio.sleep(sleep)

    logger.debug('Starting loop for line %s,%s', line, way)

    while True:
        asyncio.ensure_future(save_route(line, way, semaphore))
        await asyncio.sleep(PERIOD)


def main():
    logger.info("Starting asyncstib")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(db.connect_async(loop=loop))
    logger.debug("Postgres connection ok")

    logger.debug("Gathering network info")
    network = Network()
    # we only need line numbers and we don't want Noctis
    lines = [line.id for line in network.lines if 'N' not in str(line.id)]
    routes = [(line, 1) for line in lines] + [(line, 2) for line in lines]
    # routes = routes[:2]

    semaphore = asyncio.Semaphore(CONCURRENCY)

    logger.debug("Adding %i tasks (lines)", len(routes))
    for i, (line, way) in enumerate(routes):
        sleep = i * PERIOD / len(routes)
        asyncio.async(route_loop(sleep, line, way, semaphore))

    logger.info("Starting main loop")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info('KeyboardInterrupt')


if __name__ == '__main__':
    main()
