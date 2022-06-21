from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine
import csv

engine = create_engine('sqlite:///database.db', echo=True)

meta = MetaData()

stations = Table(
   'stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', String),
   Column('longitude', String),
   Column('elevation', String),
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

measure = Table(
   'measure', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', String),
   Column('tobs', String),
)

meta.create_all(engine)
print(engine.table_names())

conn = engine.connect()

ins_stations = stations.insert()
ins_measure = measure.insert()
  

with open('clean_stations.csv', newline='') as csvfile:
   reader = csv.DictReader(csvfile)
   for row in reader:
      conn.execute(ins_stations, [
         {
         'station': row['station'], 
         'latitude': row['latitude'],
         'longitude': row['longitude'],
         'elevation': row['elevation'],
         'name': row['name'],
         'country': row['country'],
         'state': row['state'],
         }
      ])

with open('clean_measure.csv', newline='') as csvfile:
   reader = csv.DictReader(csvfile)
   for row in reader:
      conn.execute(ins_measure, [
         {
         'station': row['station'], 
         'date': row['date'],
         'precip': row['precip'],
         'tobs': row['tobs'],
         }
      ])