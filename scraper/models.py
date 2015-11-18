from playhouse.postgres_ext import *

db = PostgresqlExtDatabase('delay', register_hstore=False)


class Heading(Model):
    line = CharField()
    way = IntegerField()
    stops = ArrayField(field_class=BooleanField)
    timestamp = DateTimeField()

    class Meta:
        database = db

# Heading.create_table()
