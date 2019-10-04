import logging
import yaml
import time
import queue
from twisted.internet import reactor
import warnings
import os

from src.stream import StreamHandler
from src.database import Database

from src import database, stream

# ######## LOGGING SETUP ###########
logpath = './logs'
loglevel = os.environ.get('LOGLEVEL', 'INFO')
database.log.setLevel(loglevel)
stream.log.setLevel(loglevel)
log = logging.getLogger(__name__)
log.setLevel(loglevel)

logFormatter = logging.Formatter('%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s')
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler('{0}/{1}.log'.format(logpath, 'harvester'))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)
# ###################################


def exit_program():
    reactor.stop()
    exit(1)


def processing_loop(q, db, symbols_to_record, timeout=60):

    symbols_to_record = [x.lower() for x in symbols_to_record]

    insert_counter = 0
    while True:
        data = q.get(timeout=timeout)

        try:
            stream_type = data['stream']
        except KeyError as err:
            log.error(f'No stream type in received data. Data: ({err})')
            continue

        timestamp = data['timestamp']

        if stream_type == '!ticker@arr':

            for item in data['data']:
                pair, close_time = item['s'], item['C']

                if pair.lower() in symbols_to_record:
                    while not db.insert_ticker(item, timestamp, ignore_if_exists=True):
                        db.reconnect()
                    log.debug(f'Inserted ticker for "{pair}" at timestamp {close_time}.')

        elif '@depth' in stream_type:
            timestamp = int(time.time())
            data['data']['timestamp'] = timestamp

            while not db.insert_depth(data, timestamp, ignore_if_exists=True):
                db.reconnect()
            log.debug(f'Inserted to table "{stream_type}" at timestamp {timestamp}.')

        else:
            w_msg = f'Unknown stream type "{stream_type}" in received data.'
            warnings.warn(w_msg)
            log.warning(w_msg)

        q.task_done()
        insert_counter += 1

        if insert_counter % 100 == 0:
            log.info(f'Insert count: {insert_counter}')


if __name__ == '__main__':

    with open('./conf/conf.yml') as f:
        cfg = yaml.safe_load(f.read())

    db_dir = cfg['db_dir']
    db_name = cfg['db_name']
    db_path = os.path.join(db_dir, db_name)

    log.info(f'Using database "{db_name}"')
    if not os.path.isfile(db_path):
        log.warning(f'Database "{db_name}" doesnt exist. It will be created.')

    dbb = Database(db_path)

    q = queue.Queue()

    def callback(data):
        data['timestamp'] = int(time.time()*1000)
        q.put(data)

    ticker_stream = StreamHandler(callback, '!ticker@arr')
    depth_streams = [f'{pair.lower()}@depth5' for pair in cfg['record_symbols']]
    depth_streams = [StreamHandler(callback, stream) for stream in depth_streams]

    try:
        processing_loop(q, dbb, cfg['record_symbols'])
    except queue.Empty:
        log.critical('Processing loop timed out.')
    except KeyboardInterrupt:
        log.info('KeyboardInterrupt received.')

    dbb.close()

    ticker_stream.close()
    for stream in depth_streams:
        stream.close()

    exit_program()
