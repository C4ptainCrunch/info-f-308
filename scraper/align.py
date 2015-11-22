import numpy as np


def load_from_file(stream):
    return np.array([[char == "■" for char in line.strip()] for line in stream])


def align(data):
    out = np.zeros(data.shape, dtype=np.int16)
    for column in reversed(range(data.shape[1])):
        pass
