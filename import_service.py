#!/usr/bin/env python3
"""
import_service.py

Fetches a single timetable from a bustimes.org service page and ingests it into the database.
Usage:
    pip install requests beautifulsoup4 sqlalchemy psycopg2-binary
    export DATABASE_URL or DB_HOST,DB_NAME,DB_USER,DB_PASSWORD
    python import_service.py SERVICE_URL
"""
import os
import sys
import requests
from bs4 import BeautifulSoup
from sqlalchemy import (
    create_engine, Column, Integer, String, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import sessionmaker, declarative_base

# Database config
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    DB_HOST = os.environ.get("DB_HOST")
    DB_NAME = os.environ.get("DB_NAME")
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_PORT = os.environ.get("DB_PORT", "5432")
    if all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    else:
        SQLITE_PATH = os.environ.get("SQLITE_PATH", "bus_times.db")
        DATABASE_URL = f"sqlite:///{SQLITE_PATH}"
        print(f"Using SQLite database at {SQLITE_PATH}")

# ORM setup
Base = declarative_base()
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Stop(Base):
    __tablename__ = "stops"
    id = Column(Integer, primary_key=True)
    external_id = Column(String, unique=True, index=True, nullable=True)
    name = Column(String, unique=True, index=True)

class Route(Base):
    __tablename__ = "routes"
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)

class Trip(Base):
    __tablename__ = "trips"
    id = Column(Integer, primary_key=True)
    route_id = Column(Integer, ForeignKey("routes.id"), index=True)
    direction = Column(String, index=True)
    __table_args__ = (UniqueConstraint("route_id", "direction", "id", name="uix_route_dir_trip"),)

class TripStop(Base):
    __tablename__ = "trip_stops"
    id = Column(Integer, primary_key=True)
    trip_id = Column(Integer, ForeignKey("trips.id"), index=True)
    stop_id = Column(Integer, ForeignKey("stops.id"), index=True)
    sequence = Column(Integer, index=True)
    time = Column(String)
    # unique constraint removed to allow same stop multiple times in a trip

# Scraper config
BASE_URL = "https://bustimes.org"

def fetch_soup(url):
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def parse_service(url):
    """
    Parse a service page and return:
      - route_name (str)
      - schedules: list of (direction_label, stops, trips)
        where stops is [(external_id, name),...]
        and trips is [[time1,...],...]
    """
    soup = fetch_soup(url)
    # Base route name
    h1 = soup.find("h1", class_="service-header")
    if h1:
        num = h1.find("strong", class_="name")
        desc = h1.find("span", class_="description")
        route_name = " ".join(filter(None, [num.get_text(strip=True) if num else "", desc.get_text(strip=True) if desc else ""]))
    else:
        route_name = url
    schedules = []
    # Find groupings
    groups = soup.find_all("div", class_="grouping")
    if groups:
        for div in groups:
            hdr = div.find(["h3", "h2", "h4"])
            direction = hdr.get_text(strip=True) if hdr else "default"
            tbl = div.find("table", class_="timetable") or div.find("table")
            if not tbl:
                continue
            stops = []
            trips_by_stop = []
            for tr in tbl.find_all("tr"):
                th = tr.find("th", {"scope": "row"}) or tr.find("th")
                if not th:
                    continue
                link = th.find("a", href=True)
                ext = None
                if link and "/stops/" in link["href"]:
                    ext = link["href"].split("/stops/")[-1]
                name = th.get_text(strip=True)
                times = [td.get_text(strip=True) or None for td in tr.find_all("td")]
                stops.append((ext, name))
                trips_by_stop.append(times)
            n_trips = max((len(x) for x in trips_by_stop), default=0)
            trips = [[times[i] if i < len(times) else None for times in trips_by_stop] for i in range(n_trips)]
            schedules.append((direction, stops, trips))
    else:
        # Single table fallback
        tbl = soup.find("table", class_="timetable") or soup.find("table")
        if not tbl:
            raise RuntimeError("No timetable table found")
        stops = []
        trips_by_stop = []
        for tr in tbl.find_all("tr"):
            th = tr.find("th", {"scope": "row"}) or tr.find("th")
            if not th:
                continue
            link = th.find("a", href=True)
            ext = None
            if link and "/stops/" in link["href"]:
                ext = link["href"].split("/stops/")[-1]
            name = th.get_text(strip=True)
            times = [td.get_text(strip=True) or None for td in tr.find_all("td")]
            stops.append((ext, name))
            trips_by_stop.append(times)
        n_trips = max((len(x) for x in trips_by_stop), default=0)
        trips = [[times[i] if i < len(times) else None for times in trips_by_stop] for i in range(n_trips)]
        schedules.append(("default", stops, trips))
    return route_name, schedules

def ingest_service(url):
    # (Re)create schema
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    db = SessionLocal()
    try:
        route_name, schedules = parse_service(url)
        print(f"Importing '{route_name}' with {sum(len(trips) for _,_,trips in schedules)} trips across {len(schedules)} direction(s)")
        # Upsert route
        route = db.query(Route).filter_by(name=route_name).first()
        if not route:
            route = Route(name=route_name)
            db.add(route)
            db.flush()
        # Insert each direction's schedule
        for direction, stops, trips in schedules:
            print(f"- Direction '{direction}': {len(trips)} trips, {len(stops)} stops")
            stop_objs = []
            for ext, name in stops:
                st = None
                if ext:
                    st = db.query(Stop).filter_by(external_id=ext).first()
                if not st:
                    st = db.query(Stop).filter_by(name=name).first()
                if not st:
                    st = Stop(external_id=ext, name=name)
                    db.add(st)
                    db.flush()
                elif ext and st.external_id != ext:
                    st.external_id = ext
                    db.flush()
                stop_objs.append(st)
            # Add trips
            for trip_times in trips:
                trip = Trip(route_id=route.id, direction=direction)
                db.add(trip)
                db.flush()
                for idx, tm in enumerate(trip_times):
                    if tm:
                        db.add(TripStop(trip_id=trip.id, stop_id=stop_objs[idx].id, sequence=idx, time=tm))
        db.commit()
        print("Ingestion complete")
    except Exception as e:
        db.rollback()
        print("Error:", e)
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: import_service.py SERVICE_URL")
        sys.exit(1)
    ingest_service(sys.argv[1])
