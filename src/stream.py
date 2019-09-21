import logging
log = logging.getLogger(__name__)
import queue
import threading
import requests
import time
from binance.websockets import BinanceSocketManager
from binance.client import Client


class StreamHandler:
    """ Handles stream setup and automatic reconnecting in case of an error. """

    def __init__(self, callback, stream_name, timeout=10):

        self._keep_alive_queue = queue.Queue()

        def _callback_wrapper(*args, **kwargs):
            self._keep_alive_queue.put(True)
            return callback(*args, **kwargs)

        self._callback = _callback_wrapper

        self._stream_name = stream_name

        self._timeout = timeout
        self._closed = False

        self._stream = None
        self._socket_manager = None
        self._keep_alive_thread = None

        self._start_stream()

    def close(self):
        self._closed = True
        self._close_stream()

    def _start_stream(self):

        do_init = True if self._socket_manager is None else False

        while self._socket_manager is None:
            try:
                self._socket_manager = BinanceSocketManager(Client(None, None))

                break
            except requests.exceptions.ConnectionError as err:
                log.error(f'Failed to connect. Retrying soon. Error: ({err})')
                time.sleep(3)
                continue
        log.debug('Socket manager created.')

        self._stream = self._socket_manager.start_multiplex_socket([self._stream_name], self._callback)

        if do_init:
            self._socket_manager.start()

        self._keep_alive_thread = threading.Thread(target=self._keep_alive_loop)
        self._keep_alive_thread.daemon = True
        self._keep_alive_thread.start()

        log.info('Streaming started.')

    def _keep_alive_loop(self):

        log.debug('Keep alive loop started.')
        while not self._closed:

            try:
                self._keep_alive_queue.get(timeout=self._timeout)
                self._keep_alive_queue.task_done()
            except queue.Empty:
                if self._closed:
                    log.info('External close received. Closing.')
                    break
                log.info('Keep alive loop timed out. Closing and restarting stream.')
                self._close_stream()
                self._start_stream()
                break

    def _close_stream(self):
        self._socket_manager.stop_socket(self._stream)

