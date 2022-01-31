"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

table = app.Table(
    "transformed_stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


def get_line(red=False, blue=False, green=False):
    if red:
        return "red"
    elif blue:
        return "blue"
    elif green:
        return "green"
    else:
        ""


@app.agent(topic)
async def process_stations(stations):
    async for station in stations.group_by(Station.station_name):
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=get_line(station.red, station.blue, station.green)
        )
        table[station.station_name] = transformed_station


if __name__ == "__main__":
    app.main()
