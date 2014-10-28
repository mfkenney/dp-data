#!/usr/bin/env python
"""
.. module:: zmq
   :synopsis: ZeroMQ message functions
"""
try:
    import simplejson as json
except ImportError:
    import json


def send_message(sock, command, params):
    """
    Send a message to the DP server.

    :param sock: ZeroMQ socket
    :param command: command string
    :param params: command parameters
    :type params: dict
    """
    sock.send_multipart([bytes(command.upper()),
                         json.dumps(params)])


def recv_message(sock):
    """
    Receive a message from the DP server.

    :param sock: ZeroMQ socket.
    :return: tuple of message-type, contents
    :rtype: tuple(string, dict)
    """
    mtype, contents = sock.recv_multipart()
    return mtype, json.dumps(contents)
