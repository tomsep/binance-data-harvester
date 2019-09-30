import logging
import yaml
import time
import queue
from twisted.internet import reactor
import warnings
import signal

from src.stream import StreamHandler
from src.database import MySQLDB


log = logging.getLogger(__name__)


class GracefulKiller:
    """ Catch termination signal
    Source: https://stackoverflow.com/a/31464349
    """
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


def exit_program():
    reactor.stop()
    exit(1)


def processing_loop(q, db, symbols_to_record, timeout=60):

    symbols_to_record = [x.lower() for x in symbols_to_record]
    kill_event = GracefulKiller()

    while not kill_event.kill_now:
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
                    db.insert_ticker(item)
                    log.debug(f'Inserted ticker for "{pair}" at event time {event_time}.')

        elif '@depth' in stream_type:
            timestamp = int(time.time())
            data['data']['timestamp'] = timestamp
            db.insert_depth(data)
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

    dbb = MySQLDB(cfg['db']['host'], cfg['db']['port'], cfg['db']['user'], cfg['db']['passwd'],
                  cfg['db']['db_name'], cfg['db']['commit_freq'])

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
    except KeyboardInterrupt:
        pass

    dbb.close()

    ticker_stream.close()
    for stream in depth_streams:
        stream.close()

    exit_program()
