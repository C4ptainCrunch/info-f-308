import numpy as np


def load_from_file(stream):
    return np.array([[char == "■" for char in line.strip()] for line in stream])



def align(data):
    out = np.zeros(data.shape, dtype=np.int16)
    x = -1
    y = 0
    i = 0
    while x >= -data.shape[1] and y < data.shape[0]:
        if data[y][x] and out[y][x] == 0:
            out[y][x] = i
            if y == 0: # apparition du véhicule au terminus
                i += 1 # nouveau véhicule
                x, y = -1, 0 # on recommence en haut à droite
            else: # on cherche la position précédente
                y = 0
                x -= 1
        else:
            y += 1
            if y == data.shape[0]:
                y = 0
                x -= 1

    return out
