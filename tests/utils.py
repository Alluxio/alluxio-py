# -*- coding: utf-8 -*-

import errno
import os


def local_path(directory, iteration_id, process_id):
    iteration = 'iteration_{}'.format(iteration_id)
    process = 'process_{}'.format(process_id)
    return os.path.join(directory, iteration, process)


def alluxio_path(directory, iteration_id, node_id, process_id):
    iteration = 'iteration_{}'.format(iteration_id)
    node = 'node_{}'.format(node_id)
    process = 'process_{}'.format(process_id)
    return os.path.join(directory, iteration, node, process)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise e
