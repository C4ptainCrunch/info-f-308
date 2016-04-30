import peewee
import peewee_asyncext

from playhouse.postgres_ext import ArrayField

db = peewee_asyncext.PooledPostgresqlExtDatabase('delay', register_hstore=False)


class Heading(peewee.Model):
    line = peewee.CharField()
    way = peewee.IntegerField()
    stops = ArrayField(field_class=peewee.BooleanField)
    timestamp = peewee.DateTimeField()

    class Meta:
        database = db

class Traject(peewee.Model):
    line = peewee.CharField()
    way = peewee.IntegerField()
    stops_times = ArrayField(field_class=peewee.DateTimeField)

    class Meta:
        database = db

if __name__ == '__main__':
    Heading.create_table()
    Traject.create_table()
