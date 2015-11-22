import numpy as np


def load_from_file(stream):
    return np.array([[char == "■" for char in line.strip()] for line in stream])



def align(data):
    out = np.zeros(data.shape, dtype=np.int16)
    x = data.shape[1] - 1
    y = 0
    i = 0
    while (x, y) != (0, data.shape[0] - 1):
        if data[y][x] and out[y][x] == 0:
            out[y][x] = i
            if y == 0: # apparition du véhicule au terminus
                i += 1 # nouveau véhicule
                x = data.shape[1] - 1 # on recommence en haut
                y = 0 # à droite
            else: # on cherche la position précédente
                y = 0
                x -= 1
        else:
            y += 1
            if y == data.shape[0]:
                y = 0
                print(x)
                x -= 1

    return out
