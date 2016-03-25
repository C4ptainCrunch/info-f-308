
# coding: utf-8

# In[191]:

import psycopg2, statistics
from collections import defaultdict
import collections
from datetime import timedelta
import matplotlib.mlab as mlab
import scipy

get_ipython().magic('pylab inline')
pylab.rcParams['figure.figsize'] = (20, 8)


# In[ ]:

conn = psycopg2.connect(database="delay", user="nikita")
conn.autocommit = True


# In[ ]:

cur = conn.cursor()
cur.execute("SELECT * FROM heading WHERE line='95' AND way=1 ORDER BY id;")
data = cur.fetchall()

stops_names = ['GRAND-PLACE', 'BIBLIOTHEQUE', 'GRAND SABLON', 'PETIT SABLON', 'ROYALE', 'TRONE', 'SCIENCE', 'LUXEMBOURG', 'IDALIE', 'BLYCKAERTS', 'GERMOIR', 'RODIN', 'DELPORTE', 'ETTERBEEK GARE', 'THYS', 'CIM. D\'IXELLES', 'RELAIS', 'ARCADES', 'KEYM', 'VANDER ELST', 'ORTOLANS', 'LES 3 TILLEULS', 'CERISAIE', 'FAUCONNERIE',]


# In[ ]:

import merger
import itertools
from datetime import timedelta

dates = [row[4] for row in data]
positions = [np.array(row[3]) for row in data]

def day_grouper(row):
    date, position = row
    date = date - timedelta(hours=3)
    return date.year, date.month, date.day



# In[ ]:

get_ipython().run_cell_magic('time', '', 'groups = itertools.groupby(zip(dates, positions), day_grouper)\nall_trajects = []\nfor day, group in groups:\n    pos = [x[1] for x in group]\n    trajects = merger.trajects_from_bool(pos)\n    trajects = map(merger.skip_terminus, trajects)\n    trajects = map(merger.reduce_traject, trajects)\n    trajects = list(trajects)\n    all_trajects += trajects')


# In[ ]:

lengths = [len(traject) for traject in all_trajects]

print("%i trajets" % len(all_trajects))
percentage = len([x for x in lengths if x < 10])*100/len(lengths)
print("%i%% font moins de 10 arrêts" % percentage)

percentage = len([x for x in lengths if x < 15])*100/len(lengths)
print("%i%% font moins de 15 arrêts" % percentage)

plt.hist(lengths, bins=range(0,23), alpha=0.7, normed=True);


# In[ ]:

def append_time(traject, all_dates):
    return {stop: all_dates[row] for row, stop in traject}

timed_trajects = [append_time(traject, dates) for traject in all_trajects]


# On calcule le temps qu'il faut aller d'un arrêt au suivant (arrondi à la seconde).
# `time_per_stop` renvoie un dictionnaire qui associe à chaque n° arrêt le temps qu'il a fallu pour arriver au suivant

# In[ ]:

def time_per_stop(traject):
    ret = {}
    for stop, time in traject.items():
        if stop + 1 in traject.keys():
            delta = traject[stop + 1] - time
            ret[stop] = round(delta.total_seconds())
    
    return ret


# In[ ]:

durations = list(map(time_per_stop, timed_trajects))


# In[ ]:

travel_times = [[] for _ in range(23)]
for duration_dict in durations:
    for stop, duration in duration_dict.items():
        travel_times[stop].append(duration)


# In[ ]:

plt.boxplot(travel_times, showfliers=False)
plt.title("Temps de trajet entre 2 arrêts du 95 en fonction de l'arrêt")
plt.xlabel('Arrêts')
plt.ylabel('Temps de trajet (s)');


# In[ ]:

fig, ax = plt.subplots(figsize=(20,8))
ax.grid(True)

xf = defaultdict(str,enumerate(stops_names))
ax.xaxis.set_major_formatter(FuncFormatter(lambda tick, pos: xf[int(tick)])) 
ax.xaxis.set_major_locator(IndexLocator(2, 0)) 

plt.suptitle("Nombre d'échantillons par arrêt");
plt.grid()
plt.bar(range(len(travel_times)),[len(x) for x in travel_times]);


# In[ ]:

for stop, times in enumerate(travel_times):
    mu = statistics.mean(times)
    sigma = statistics.stdev(times)
    n, bins, patches = plt.hist(times, bins=range(0,400, 20), alpha=0.7, normed=True);

    bins = array(range(0, max(bins)))
    y = mlab.normpdf(bins, mu, sigma)
    plt.plot(bins, y, 'r-', linewidth=3)
    plt.suptitle("Temps de trajet entre l'arrêt {} et {}".format(stops_names[stop], stops_names[stop+1]))
    plt.show()

