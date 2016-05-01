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


###############################
## Main
###############################

def traject_to_timestamps(traject, dates):
    timestamps = [None] * (max(traject, key=lambda x: x[1])[1] + 1)
    for when, stop in traject:
        timestamps[stop] = dates[when]

    return timestamps

def timestamps_to_model(timestamps, line, way):
    return {
        'line' : line,
        'way' : way,
        'stops_times' : timestamps,
        'start_time' : min((i for i in timestamps if i is not None))
    }

def save_day(arg):
    try:
        row_list, line, way = arg
        dates, positions = zip(*row_list)

        trajects = trajects_from_bool(positions)
        trajects = map(skip_terminus, trajects)
        trajects = map(reduce_traject, trajects)

        timestamps = (traject_to_timestamps(t, dates) for t in trajects)
        models = [timestamps_to_model(t, line, way) for t in timestamps]

        if len(models) > 0:
            with db.atomic():
                Traject.insert_many(models).execute()
    except Exception as e:
        print(line, way, e)


if __name__ == '__main__':
    print("Merging lines...")
    from constants import LINES
    from itertools import groupby
    from models import db, Traject
    from multiprocessing import Pool

    p = Pool(4)
    # LINES = [1,2,3,4,5,6,7,94,25]
    routes = [(line, 1) for line in LINES] + [(line, 2) for line in LINES]
    routes = [(12, 1), (12, 2), (13, 1), (13, 2), (14, 1), (14, 2), (15, 1), (15, 2), (19, 1), (19, 2), (20, 1), (20, 2), (21, 1), (21, 2), (22, 1), (22, 2), (25, 2), (27, 1), (27, 2), (28, 1), (28, 2), (29, 1), (29, 2), (32, 1), (32, 2), (34, 1), (34, 2), (36, 1), (36, 2), (38, 1), (38, 2), (39, 1), (39, 2), (41, 1), (41, 2), (42, 1), (42, 2), (43, 1), (43, 2), (44, 1), (44, 2), (45, 1), (45, 2), (46, 1), (46, 2), (47, 1), (47, 2), (48, 1), (48, 2), (49, 1), (49, 2), (50, 1), (50, 2), (51, 1), (51, 2), (53, 1), (53, 2), (54, 1), (54, 2), (55, 1), (55, 2), (57, 1), (57, 2), (58, 1), (58, 2), (59, 1), (59, 2), (60, 1), (60, 2), (61, 1), (61, 2), (62, 1), (62, 2), (63, 1), (63, 2), (64, 1), (64, 2), (65, 1), (65, 2), (66, 1), (66, 2), (69, 1), (69, 2), (71, 1), (71, 2), (72, 1), (72, 2), (75, 1), (75, 2), (76, 1), (76, 2), (77, 1), (77, 2), (78, 1), (78, 2), (79, 1), (79, 2), (80, 1), (80, 2), (81, 1), (81, 2), (82, 1), (82, 2), (84, 1), (84, 2), (86, 1), (86, 2), (87, 1), (87, 2), (88, 1), (88, 2), (89, 1), (89, 2), (92, 1), (92, 2), (93, 1), (93, 2), (97, 1), (97, 2), (98, 1), (98, 2)]
    for line, way in routes:
        print("Line", line, "way", way)
        dates, positions = get_data_from_db(line, way)
        iterable = zip(dates, positions)

        # TODO : décaler de 3 heures pour ne pas couper les bus qui roulment après minuit
        groups = [(list(v), line, way) for k, v in groupby(iterable, lambda x: x[0].date())]
        p.map_async(save_day, groups)

    print("Waiting to close")
    p.close()

