from models import Heading
from stib.stib import Traject

from datetime import datetime, timedelta
import time
import sys


def every(timedelta):
    next_ = start = datetime.now()
    while True:
        next_ += timedelta
        yield next_


def sleep_until(target):
    now = datetime.now()
    delta = target - now
    if delta.total_seconds() > 0:
        time.sleep(delta.total_seconds())


traject = Traject(94, 1)
for next_loop in every(timedelta(seconds=20)):
    try:
        traject.update()
        stops = map(lambda x: x.present, traject.stops)
        h = Heading(line=str(traject.id), way=str(traject.way), stops=stops, timestamp=traject.last_update)
        h.save()
        print(".", end="")
        sys.stdout.flush()
    except Execption as e:
        print(datetimee.now(), e)

    sleep_until(next_loop)
