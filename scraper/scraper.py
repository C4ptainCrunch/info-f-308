from models import Heading
from stib import Traject

traject = Traject(5, 1)
stops = map(lambda x: x.present, traject.stops)
h = Heading(line=str(traject.id), way=str(traject.way), stops=stops, timestamp=traject.last_update)
