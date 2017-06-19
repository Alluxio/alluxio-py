# -*- coding: utf-8 -*-

import os


def local_path(directory, process_id):
    return os.path.join(directory, 'process_{}'.format(process_id))


def alluxio_path(directory, node_id, process_id):
    node = 'node_{}'.format(node_id)
    process = 'process_{}'.format(process_id)
    return os.path.join(directory, node, process)
