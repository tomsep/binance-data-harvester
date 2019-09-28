import logging
import yaml
import time
import os
import queue
from twisted.internet import reactor
import warnings

from src.stream import StreamHandler
from src.database import DataBase


log = logging.getLogger(__name__)


def exit_program():
    reactor.stop()
    exit(1)


def processing_loop(q, db, symbols_to_record, timeout=60):

    symbols_to_record = [x.lower() for x in symbols_to_record]

    while True:
        data = q.get(timeout=timeout)
        q.task_done()
        try:
            stream_type = data['stream']
        except KeyError as err:
            log.error(f'No stream type in received data. Data: ({err})')
            continue

        if stream_type == '!ticker@arr':
            for item in data['data']:
                pair, event_time = item['s'], item['E']
                if pair.lower() in symbols_to_record:
                    db.insert(f'{pair}@24hrTicker', item)
                    log.debug(f'Inserted ticker for "{pair}" at event time {event_time}.')

        elif '@depth' in stream_type:
            timestamp = int(time.time())
            data['data']['timestamp'] = timestamp
            db.insert(stream_type, data['data'])
            log.debug(f'Inserted to table "{stream_type}" at timestamp {timestamp}.')

        else:
            w_msg = f'Unknown stream type "{stream_type}" in received data.'
            warnings.warn(w_msg)
            log.warning(w_msg)


if __name__ == '__main__':

    with open('./conf.yml') as f:
        cfg = yaml.safe_load(f.read())

    logging.basicConfig()
    log.setLevel(cfg['log_level'])

    with open('./db_field_names.yml') as f:
        field_naming_rules = yaml.safe_load(f.read())

    db_abs_path = os.path.abspath(cfg['db_path'])

    if not os.path.isfile(db_abs_path):
        input(f'Database doesn´t exist. Path:\n{db_abs_path}\nPress enter to create and continue: ')

    log.info(f'Using database at {db_abs_path}')

    dbb = DataBase(db_abs_path, field_naming_rules, cfg['commit_freq'])

    q = queue.Queue()

    def callback(data):
        q.put(data)

    ticker_stream = StreamHandler(callback, '!ticker@arr')
    depth_streams = [f'{pair.lower()}@depth20' for pair in cfg['record_symbols']]
    depth_streams = [StreamHandler(callback, stream) for stream in depth_streams]

    try:
        processing_loop(q, dbb, cfg['record_symbols'])
    except queue.Empty:
        log.critical('Processing loop timed out.')

    ticker_stream.close()
    for stream in depth_streams:
        stream.close()

    exit_program()
