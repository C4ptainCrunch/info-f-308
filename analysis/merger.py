import psycopg2
import collections
import itertools
import numpy as np
from datetime import timedelta

def get_data_from_db(line, way):
    conn = psycopg2.connect(database="delay", user="nikita")
    conn.autocommit = True

    cur = conn.cursor()
    cur.execute("SELECT * FROM heading WHERE line='%i' AND way=%i ORDER BY id;" % (line, way))
    data = cur.fetchall()

    dates = [row[4] for row in data]
    positions = [np.array(row[3]) for row in data]

    return dates, positions


V_TRESHOLD = 7
H_TRESHOLD = 5

def assign_id_to_row(aligned, row, max_id=0):
    identified_row = np.zeros_like(row, dtype=np.int64)
    if aligned is None:
        for i, has_bus in reversed(list(enumerate(row))):
            if has_bus:
                max_id += 1
                identified_row[i] = max_id
        return identified_row, max_id

    for i, has_bus in reversed(list(enumerate(row))):
        if has_bus:
            for j in range(i, max(i - H_TRESHOLD, -1), -1):
                if identified_row[i]:
                    break
                # top to bottom
                for k in range(max(aligned.shape[0] - V_TRESHOLD, 0), aligned.shape[0]):

                # bottom to top
                #for k in range(aligned.shape[0] - 1, max(aligned.shape[0] - V_TRESHOLD, -1), -1):
                    current_cell = aligned[k][j]
                    if (current_cell != 0
                        and current_cell not in aligned[k+1:]
                        and  current_cell not in identified_row
                       ):
                        identified_row[i] = current_cell
                        break

            if identified_row[i] == 0:
                max_id += 1
                identified_row[i] = max_id

    return identified_row, max_id


def trajects_from_bool(data):
    aligned = None
    max_id = 0
    for row in data:
        identified_row, max_id = assign_id_to_row(aligned, row, max_id=max_id)
        if aligned is None:
            aligned = np.array([identified_row])
        else:
            aligned = np.vstack((aligned, identified_row))

    buses = collections.defaultdict(list)
    for index, x in np.ndenumerate(aligned):
        if x != 0:
            buses[x].append(index)

    return buses.values()


def skip_terminus(traject):
    index = -1
    for i, l in enumerate(traject):
        time, stop = l
        if stop == 0:
            index = i
        else:
            break
    last_terminus = index
    if last_terminus >= 0:
        return traject[last_terminus:]
    else:
        return traject


def reduce_traject(traject):
    seen_stops = set()
    ret = []
    for time, stop in traject:
        if stop not in seen_stops:
            ret.append([time, stop])
            seen_stops.add(stop)
    return ret


def append_time(traject, all_dates):
    return {stop: all_dates[row] for row, stop in traject}

MIN_LEN = 3

def extract_trajects(dates, positions):
    def day_grouper(row):
        date, position = row
        date = date - timedelta(hours=3)
        return date.year, date.month, date.day

    groups = itertools.groupby(zip(dates, positions), day_grouper)

    all_trajects = []
    for day, group in groups:
        group = list(group)

        pos = [x[1] for x in group]
        day_dates = [x[0] for x in group]

        trajects = trajects_from_bool(pos)
        trajects = map(skip_terminus, trajects)
        trajects = map(reduce_traject, trajects)
        trajects = [append_time(traject, day_dates) for traject in trajects if len(traject) > MIN_LEN]
        all_trajects += trajects

    return all_trajects


def time_per_stop(traject):
    ret = {}
    for stop, time in traject.items():
        if stop + 1 in traject.keys():
            delta = traject[stop + 1] - time
            ret[stop] = round(delta.total_seconds())

    return ret
