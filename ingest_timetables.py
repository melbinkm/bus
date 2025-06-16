#!/usr/bin/env python3
"""
ingest_timetables.py

Fetches all timetables from bustimes.org/regions/IM under class="services"
and ingests them into the database.
"""
import sys
import requests
from bs4 import BeautifulSoup
from import_service import Base, engine, SessionLocal, Stop, Route, Trip, TripStop, parse_service
from config import BASE_URL, REGION_PATH

def get_service_urls():
    """Return sorted list of service URL paths under the region page."""
    url = BASE_URL + REGION_PATH
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    # Look for <ul class="services"> containing service links
    ul = soup.find("ul", class_="services")
    if not ul:
        raise RuntimeError(f"No services list found at {url}")
    hrefs = [a["href"] for a in ul.find_all("a", href=True)]
    return sorted(set(hrefs))

def main():
    # (Re)create schema once
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    session = SessionLocal()
    try:
        services = get_service_urls()
        print(f"Found {len(services)} services to ingest")
        for href in services:
            full_url = BASE_URL + href
            print(f"Ingesting {full_url}")
            route_name, schedules = parse_service(full_url)
            # Upsert route
            route = session.query(Route).filter_by(name=route_name).first()
            if not route:
                route = Route(name=route_name)
                session.add(route)
                session.flush()
            # Ingest each direction
            for direction, stops, trips in schedules:
                # Upsert stops
                stop_objs = []
                for ext, name in stops:
                    st = None
                    if ext:
                        st = session.query(Stop).filter_by(external_id=ext).first()
                    if not st:
                        st = session.query(Stop).filter_by(name=name).first()
                    if not st:
                        st = Stop(external_id=ext, name=name)
                        session.add(st)
                        session.flush()
                    elif ext and st.external_id != ext:
                        st.external_id = ext
                        session.flush()
                    stop_objs.append(st)
                # Add trips
                for trip_times in trips:
                    trip = Trip(route_id=route.id, direction=direction)
                    session.add(trip)
                    session.flush()
                    for idx, tm in enumerate(trip_times):
                        if tm:
                            session.add(TripStop(
                                trip_id=trip.id,
                                stop_id=stop_objs[idx].id,
                                sequence=idx,
                                time=tm
                            ))
            session.commit()
        print("All services ingested successfully")
    except Exception as e:
        session.rollback()
        print("Error during ingestion:", e, file=sys.stderr)
        sys.exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    main()
