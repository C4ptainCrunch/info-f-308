import peewee
import peewee_async
from playhouse.postgres_ext import ArrayField

db = peewee_async.PostgresqlDatabase('delay')


class Heading(peewee.Model):
    line = peewee.CharField()
    way = peewee.IntegerField()
    stops = ArrayField(field_class=peewee.BooleanField)
    timestamp = peewee.DateTimeField()

    class Meta:
        database = db
