import numpy as np


def load_from_file(stream):
    return np.array([[char == "■" for char in line.strip()] for line in stream])


def align(data):
    #  my matrix (data) is like this
    #      time (x)
    #      --------->
    #  s | 000000000100000
    #  t | 000100010101010
    #  o | 000001000101010
    #  p | 000000000100000
    #  s | 000100010101010
    # (y)v 000001000101010

    out = np.zeros(data.shape, dtype=np.int16)

    current_id = 0

    for x in range(data.shape[1]):
        used_ids = set()
        for y in range(data.shape[0]):
            if data[y][x]:
                # top-left
                if x > 0 and y > 0 and data[y - 1][x - 1] and out[y - 1][x - 1] not in used_ids:
                    my_id = out[y - 1][x - 1]
                    used_ids.add(my_id)
                    out[y][x] = my_id

                # left
                elif x > 0 and data[y][x - 1] and out[y][x - 1] not in used_ids:
                    my_id = out[y][x - 1]
                    used_ids.add(my_id)
                    out[y][x] = my_id

                # top-top-left
                elif x > 0 and y > 1 and data[y - 2][x - 1] and out[y - 2][x - 1] not in used_ids:
                    my_id = out[y - 2][x - 1]
                    used_ids.add(my_id)
                    out[y][x] = my_id

                # new
                else:
                    current_id += 1
                    used_ids.add(current_id)
                    out[y][x] = current_id

    return out
