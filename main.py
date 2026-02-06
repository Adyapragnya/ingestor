# # main.py
# import asyncio
# import json
# import logging
# import contextlib
# from datetime import datetime, timedelta, timezone

# import websockets
# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from motor.motor_asyncio import AsyncIOMotorClient
# from pydantic_settings import BaseSettings, SettingsConfigDict
# # from apscheduler.schedulers.asyncio import AsyncIOScheduler
# # from apscheduler.triggers.interval import IntervalTrigger

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("ais-ingestor")

# # -------------------------------------------------------------------
# # Settings
# # -------------------------------------------------------------------

# class Settings(BaseSettings):
#     MONGO_URI: str
#     AIS_API_SOURCE: str = "aisstreamio"          # collection key for maritimeapikey
#     HEARTBEAT_INTERVAL_SECONDS: int = 10         # keepalive ping to upstream
#     MAX_MERGE_THRESHOLD_MIN: int = 500           # used for merge window

#     model_config = SettingsConfigDict(
#         env_file=".env",
#         env_file_encoding="utf-8",
#     )

# settings = Settings()

# # -------------------------------------------------------------------
# # FastAPI app
# # -------------------------------------------------------------------

# app = FastAPI(title="AIS Fixed Region Ingestor")

# # Optional – in case you later add HTTP endpoints used by other services
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
# THRESHOLDS = [10, 20, 30, 40, 60, 250, 500]  # minutes

# # -------------------------------------------------------------------
# # Helpers
# # -------------------------------------------------------------------

# def parse_iso(ts):
#     """
#     Convert ISO string (possibly with 'Z') or datetime to tz-aware UTC datetime.
#     """
#     if isinstance(ts, str):
#         ts = ts.replace("Z", "+00:00")
#         dt = datetime.fromisoformat(ts)
#         return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
#     elif isinstance(ts, datetime):
#         return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
#     return None


# async def ensure_indexes():
#     """
#     Create indexes on the fixed-region collections.
#     """
#     db = app.state.mongodb

#     fixed_positions_coll = db["fixedRegionAISstreamiovesselposition"]
#     fixed_statics_coll   = db["fixedRegionAISstreamiovesselsstatic"]
#     fixed_final_coll     = db["fixedRegionAISstreamiovesselsfinalized"]

#     await fixed_positions_coll.create_index("mmsi", unique=True)
#     await fixed_statics_coll.create_index("mmsi", unique=True)
#     await fixed_final_coll.create_index("mmsi", unique=True)

#     # You can also add geospatial index if needed:
#     # await fixed_positions_coll.create_index([("location", "2dsphere")])

#     logger.info("MongoDB indexes ensured for fixed-region collections.")


# async def get_fixed_bbox():
#     """
#     Fetch latest polygon from AISfixedpolygon.
#     The 'coordinates' field is assumed as [[lon,lat], ...] (closed ring optional).
#     Returns (min_lat, min_lon, max_lat, max_lon).
#     """
#     db = app.state.mongodb
#     doc = await db["AISfixedpolygon"].find_one(sort=[("_id", -1)])
#     if not doc or not doc.get("coordinates"):
#         raise HTTPException(500, "AISfixedpolygon not configured")

#     coords = doc["coordinates"]  # [[lon,lat], ...]
#     lons = [c[0] for c in coords]
#     lats = [c[1] for c in coords]
#     return (min(lats), min(lons), max(lats), max(lons))

# async def connect_with_retries(url, retries=5, delay=3):
#     attempt = 0
#     backoff = delay
#     while True:
#         try:
#             logger.info(f"Connecting to AIS stream (attempt {attempt + 1})...")
#             # NOTE: disable internal ping/pong keepalive
#             ws = await websockets.connect(
#                 url,
#                 ping_interval=None,   # <-- key change
#                 ping_timeout=None,    # optional; no timeout
#             )
#             logger.info("Connected to AIS stream.")
#             return ws
#         except Exception as e:
#             attempt += 1
#             if attempt >= retries:
#                 logger.warning(
#                     f"Failed to connect after {retries} attempts: {e}. "
#                     f"Will keep retrying every {backoff} seconds."
#                 )
#                 attempt = 0  # loop forever
#             else:
#                 logger.warning(f"Connection failed: {e}. Retrying in {backoff} seconds...")
#             await asyncio.sleep(backoff)
#             backoff = min(backoff * 2, 60)

# # async def app_keepalive(ws, interval=None):
# #     """
# #     Periodically send heartbeat messages to upstream to keep the connection alive.
# #     """
# #     if interval is None:
# #         interval = settings.HEARTBEAT_INTERVAL_SECONDS

# #     count = 0
# #     while True:
# #         await asyncio.sleep(interval)
# #         try:
# #             payload = {"type": "heartbeat", "count": count}
# #             await ws.send(json.dumps(payload))
# #             count += 1
# #             logger.debug(f"Sent heartbeat {count}")
# #         except websockets.ConnectionClosed:
# #             logger.info("Upstream websocket closed during heartbeat.")
# #             break
# #         except Exception as e:
# #             logger.warning(f"Error sending heartbeat: {e}")
# #             break

# # -------------------------------------------------------------------
# # Merge logic (your existing merge_fixed_region_vessels)
# # -------------------------------------------------------------------

# async def merge_fixed_region_vessels():
#     db = app.state.mongodb

#     pos_coll = db["fixedRegionAISstreamiovesselposition"]
#     statics_coll = db["fixedRegionAISstreamiovesselsstatic"]
#     final_coll = db["fixedRegionAISstreamiovesselsfinalized"]

#     now = datetime.utcnow().replace(tzinfo=timezone.utc)
#     since = now - timedelta(minutes=max(THRESHOLDS))

#     recent_positions = await pos_coll.find(
#         {"timestamp": {"$gte": since.isoformat()}}
#     ).to_list(None)

#     for pos in recent_positions:
#         mmsi = pos.get("mmsi")
#         ts = pos.get("timestamp")
#         if not mmsi or not ts:
#             continue

#         pos_time = parse_iso(ts)
#         if not pos_time:
#             continue

#         # get latest static for same MMSI within thresholds
#         static_candidates = (
#             await statics_coll.find({"mmsi": mmsi})
#             .sort("timestamp", -1)
#             .limit(5)
#             .to_list(None)
#         )

#         static_doc = None
#         static_t = None
#         for st in static_candidates:
#             st_ts = st.get("timestamp")
#             static_time = parse_iso(st_ts)
#             if not static_time:
#                 continue
#             diff_min = abs((pos_time - static_time).total_seconds()) / 60.0
#             t = next((thr for thr in THRESHOLDS if diff_min <= thr), None)
#             if t is not None:
#                 static_doc = st
#                 static_t = t
#                 break

#         if not static_doc:
#             continue

#         await final_coll.update_one(
#             {"mmsi": mmsi},
#             {
#                 "$set": {
#                     "merged_at": datetime.now(timezone.utc).isoformat(),
#                     "threshold_minutes": static_t,
#                     "threshold_flag": f"≤{static_t}m" if static_t else None,
#                     "time_diff_minutes": diff_min,
#                     "timestamp_position": pos["timestamp"],
#                     "timestamp_static": static_doc.get("timestamp"),
#                     "position_data": pos,
#                     "static_data": static_doc,
#                 }
#             },
#             upsert=True,
#         )

#     logger.info("merge_fixed_region_vessels run completed.")

# async def merge_latest_for_mmsi(mmsi: str):
#     """
#     Re-compute the merged document for a single MMSI immediately.

#     Uses the latest position + latest static for that MMSI,
#     applies the same threshold logic, and updates final_coll.
#     """
#     db = app.state.mongodb

#     pos_coll    = db["fixedRegionAISstreamiovesselposition"]
#     statics_coll= db["fixedRegionAISstreamiovesselsstatic"]
#     final_coll  = db["fixedRegionAISstreamiovesselsfinalized"]

#     # Get latest position for this MMSI
#     pos = await pos_coll.find_one({"mmsi": mmsi})
#     if not pos:
#         return  # no position yet

#     ts = pos.get("timestamp")
#     pos_time = parse_iso(ts)
#     if not pos_time:
#         return

#     # Ignore very old positions (same 500-minute rule)
#     now = datetime.utcnow().replace(tzinfo=timezone.utc)
#     since = now - timedelta(minutes=max(THRESHOLDS))
#     if pos_time < since:
#         return

#     # Get latest few statics
#     static_candidates = (
#         await statics_coll.find({"mmsi": mmsi})
#         .sort("timestamp", -1)
#         .limit(5)
#         .to_list(None)
#     )

#     static_doc = None
#     static_t = None
#     diff_min = None

#     for st in static_candidates:
#         st_ts = st.get("timestamp")
#         static_time = parse_iso(st_ts)
#         if not static_time:
#             continue

#         diff_min = abs((pos_time - static_time).total_seconds()) / 60.0
#         t = next((thr for thr in THRESHOLDS if diff_min <= thr), None)
#         if t is not None:
#             static_doc = st
#             static_t = t
#             break

#     if not static_doc:
#         # no suitable static within thresholds; nothing to finalize yet
#         return

#     await final_coll.update_one(
#         {"mmsi": mmsi},
#         {
#             "$set": {
#                 "merged_at": datetime.now(timezone.utc).isoformat(),
#                 "threshold_minutes": static_t,
#                 "threshold_flag": f"≤{static_t}m" if static_t else None,
#                 "time_diff_minutes": diff_min,
#                 "timestamp_position": pos["timestamp"],
#                 "timestamp_static": static_doc.get("timestamp"),
#                 "position_data": pos,
#                 "static_data": static_doc,
#             }
#         },
#         upsert=True,
#     )

#     # logger.info(f"Merged immediately for MMSI={mmsi}, threshold={static_t}, diff={diff_min:.2f} min")


# async def merge_loop_worker():
#     """
#     Background loop:
#     - Run merge_fixed_region_vessels()
#     - Wait 3 minutes
#     - Repeat

#     Ensures there is NEVER overlap: next run only starts
#     after the previous has fully completed.
#     """
#     while True:
#         try:
#             await merge_fixed_region_vessels()
#         except Exception as e:
#             logger.exception("Error in merge loop worker: %s", e)

#         # wait 3 minutes before next run
#         await asyncio.sleep(10 * 60)

# # -------------------------------------------------------------------
# # AIS background worker (NO FastAPI WebSocket, runs forever)
# # -------------------------------------------------------------------

# async def ais_fixed_region_worker():
#     """
#     Background task:
#     - Fetch AISfixedpolygon => bounding box.
#     - Connect to aisstream.io.
#     - Consume PositionReport / ShipStaticData.
#     - Store into:
#         fixedRegionAISstreamiovesselposition
#         fixedRegionAISstreamiovesselsstatic
#     - No frontend call needed.
#     """
#     db = app.state.mongodb
#     positions_coll = db["fixedRegionAISstreamiovesselposition"]
#     statics_coll   = db["fixedRegionAISstreamiovesselsstatic"]

#     api_key_doc = await db["maritimeapikey"].find_one({"source": settings.AIS_API_SOURCE})
#     if not api_key_doc:
#         logger.error("aisstreamio API key not configured in maritimeapikey collection.")
#         # do not crash the app, but keep retrying later
#     userkey = api_key_doc["key"] if api_key_doc else None

#     while True:
#         try:
#             if not userkey:
#                 # Try to reload API key if it was missing at startup
#                 api_key_doc = await db["maritimeapikey"].find_one({"source": settings.AIS_API_SOURCE})
#                 if not api_key_doc:
#                     logger.error("Still no API key in maritimeapikey. Waiting 30 seconds...")
#                     await asyncio.sleep(30)
#                     continue
#                 userkey = api_key_doc["key"]

#             min_lat, min_lon, max_lat, max_lon = await get_fixed_bbox()
#             logger.info(
#                 f"Using fixed bounding box: "
#                 f"lat[{min_lat}, {max_lat}], lon[{min_lon}, {max_lon}]"
#             )

#             ws = await connect_with_retries(AISSTREAM_URL)

#             # start heartbeat
#             # hb_task = asyncio.create_task(app_keepalive(ws))

#             bounding_box = [[[min_lat, min_lon], [max_lat, max_lon]]]
#             subscribe_payload = {
#                 "APIKey": userkey,
#                 "BoundingBoxes": bounding_box,
#             }
#             await ws.send(json.dumps(subscribe_payload))
#             logger.info("Subscription message sent to AIS stream.")

#             # main receive loop
#             while True:
#                 try:
#                     raw = await ws.recv()
#                 except websockets.ConnectionClosed as e:
#                     logger.warning(f"Upstream WebSocket closed: {e}. Reconnecting...")
#                     break
#                 except asyncio.CancelledError:
#                     logger.info("AIS worker cancelled. Closing upstream connection.")
#                     raise

#                 try:
#                     msg = json.loads(raw)
#                 except json.JSONDecodeError:
#                     logger.warning("Received invalid JSON from AIS stream.")
#                     continue

#                 message_type = msg.get("MessageType")
#                 meta = msg.get("MetaData", {}) or {}
#                 now_iso = datetime.now(timezone.utc).isoformat()

#                 if message_type == "PositionReport":
#                     report = (msg.get("Message") or {}).get("PositionReport") or {}
#                     if not report:
#                         continue

#                     mmsi = (
#                         report.get("UserID")
#                         # or report.get("MMSI")
#                         or meta.get("MMSI")
#                     )

#                     # print('Received ShipStaticData for UserID:', report.get("UserID"))
#                     # print('Received ShipStaticData for MMSI from meta:', meta.get("MMSI"))

#                     lat = report.get("Latitude") or report.get("latitude")
#                     lon = report.get("Longitude") or report.get("longitude")

#                     if not mmsi or lat is None or lon is None:
#                         continue

#                     doc = {
#                         "timestamp": now_iso,
#                         "type": "PositionReport",
#                         "mmsi": mmsi,
#                         "time_utc": meta.get("time_utc"),
#                         "metadata": meta,
#                         "location": {
#                             "type": "Point",
#                             "coordinates": [lon, lat],
#                         },
#                         "raw_report": report,
#                     }

#                     await positions_coll.update_one(
#                         {"mmsi": mmsi},
#                         {"$set": doc},
#                         upsert=True,
#                     )

#                     try:
#                         asyncio.create_task(merge_latest_for_mmsi(mmsi))
#                     except Exception as e:
#                         logger.exception(f"Immediate merge failed for MMSI={mmsi}: {e}")

#                 elif message_type == "ShipStaticData":
#                     report = (msg.get("Message") or {}).get("ShipStaticData") or {}
#                     if not report:
#                         continue

#                     mmsi = (
#                         report.get("UserID")
#                         # or report.get("MMSI")
#                         or meta.get("MMSI")
#                     )
#                     # print('Received ShipStaticData for UserID:', report.get("UserID"))
#                     # print('Received ShipStaticData for MMSI from meta:', meta.get("MMSI"))

#                     if not mmsi:
#                         continue

#                     static_doc = {
#                         "timestamp": now_iso,
#                         "type": "ShipStaticData",
#                         "mmsi": mmsi,
#                         "time_utc": meta.get("time_utc"),
#                         "metadata": meta,
#                         "raw_report": report,
#                     }

#                     await statics_coll.update_one(
#                         {"mmsi": mmsi},
#                         {"$set": static_doc},
#                         upsert=True,
#                     )

#                     try:
#                         asyncio.create_task(merge_latest_for_mmsi(mmsi))
#                     except Exception as e:
#                         logger.exception(f"Immediate merge failed for MMSI={mmsi}: {e}")

#         except asyncio.CancelledError:
#             # graceful shutdown
#             logger.info("AIS fixed-region worker task cancelled. Exiting loop.")
#             break
#         except Exception as e:
#             logger.exception(f"Stream-level error in AIS worker: {e}")
#             await asyncio.sleep(2)  # small backoff before reconnect

# # -------------------------------------------------------------------
# # FastAPI lifecycle
# # -------------------------------------------------------------------

# @app.on_event("startup")
# async def startup_event():
#     # connect Mongo
#     try:
#         app.state.mongodb_client = AsyncIOMotorClient(settings.MONGO_URI)
#         db_name = app.state.mongodb_client.get_default_database().name
#         app.state.mongodb = app.state.mongodb_client[db_name]
#         logger.info(f"Connected to MongoDB database '{db_name}'.")
#     except Exception as e:
#         logger.error(f"Failed to connect to MongoDB: {e}")
#         raise RuntimeError(f"Failed to connect to MongoDB: {e}")

#     # indexes
#     await ensure_indexes()

#     # start merge loop worker (no APScheduler)
#     app.state.merge_task = asyncio.create_task(merge_loop_worker())
#     logger.info("Merge loop worker started (runs, then waits 3 min, then runs again).")

#     # start AIS worker
#     app.state.ais_task = asyncio.create_task(ais_fixed_region_worker())
#     logger.info("AIS fixed-region background worker started.")


# @app.on_event("shutdown")
# async def shutdown_event():
#     # cancel merge loop task
#     merge_task = getattr(app.state, "merge_task", None)
#     if merge_task:
#         merge_task.cancel()
#         with contextlib.suppress(asyncio.CancelledError):
#             await merge_task
#         logger.info("Merge loop worker task cancelled.")

#     # cancel AIS worker task
#     ais_task = getattr(app.state, "ais_task", None)
#     if ais_task:
#         ais_task.cancel()
#         with contextlib.suppress(asyncio.CancelledError):
#             await ais_task
#         logger.info("AIS worker task cancelled.")

#     # close Mongo
#     client = getattr(app.state, "mongodb_client", None)
#     if client:
#         client.close()
#         logger.info("MongoDB client closed.")


# # Simple health endpoint (optional)
# @app.get("/health")
# async def health():
#     return {"status": "ok"}

# @app.get("/debug/ingest-status")
# async def ingest_status():
#     db = app.state.mongodb
#     pos_count = await db["fixedRegionAISstreamiovesselposition"].count_documents({})
#     static_count = await db["fixedRegionAISstreamiovesselsstatic"].count_documents({})
#     final_count = await db["fixedRegionAISstreamiovesselsfinalized"].count_documents({})
#     return {
#         "position_count": pos_count,
#         "static_count": static_count,
#         "finalized_count": final_count,
#     }


# main.py
from logging.handlers import RotatingFileHandler
import os 
import asyncio
import json
import logging
import contextlib
import math
from datetime import datetime, timedelta, timezone

import websockets
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ais-ingestor")

# -------------------------------------------------------------------
# Settings
# -------------------------------------------------------------------

class Settings(BaseSettings):
    # Main DB (your current one)
    MONGO_URI: str

    # Hyla DB (ports live here)
    HYLA_MONGO_URI: str

    AIS_API_SOURCE: str = "aisstreamio"          # collection key for maritimeapikey
    HEARTBEAT_INTERVAL_SECONDS: int = 10
    MAX_MERGE_THRESHOLD_MIN: int = 500

    MERGE_WORKERS: int = 16   # start with 8, increase if CPU/DB can handle
    MERGE_QUEUE_MAX: int = 100000
    # Port subscription controls
    PORT_RADIUS_KM: float = 100.0
    PORTS_MAX_BOXES: int = 60
    SUBSCRIPTION_REFRESH_SECONDS: int = 600  # 10 minutes

    USE_FIXED_POLYGON: bool = True
    USE_PORT_BOXES: bool = True

    # Hyla collection name
    HYLA_PORTS_COLLECTION: str = "ports"
    AIS_PORT_OVERRIDES_COLLECTION: str = "ais_port_watch"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

settings = Settings()

# -------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------

app = FastAPI(title="AIS Fixed Region Ingestor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
THRESHOLDS = [10, 20, 30, 40, 60, 250, 500]  # minutes

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def parse_iso(ts):
    """
    Convert ISO string (possibly with 'Z') or datetime to tz-aware UTC datetime.
    """
    if isinstance(ts, str):
        ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    elif isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return None

def setup_file_logging():
    os.makedirs("logs", exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # ✅ wipe existing handlers to avoid duplicates on reload
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = RotatingFileHandler(
        "logs/ais-ingestor.log",
        maxBytes=25 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    # optional: reduce uvicorn noise or keep it
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def bbox_around_point_km(lat: float, lon: float, radius_km: float):
    """
    Approximate a radius-km circle by a bounding box.

    Returns (min_lat, min_lon, max_lat, max_lon).
    """
    # ~111.32 km per degree latitude
    dlat = radius_km / 111.32

    # Longitude degrees scale by cos(latitude)
    cos_lat = math.cos(math.radians(lat))
    # avoid divide-by-zero near poles
    if abs(cos_lat) < 1e-6:
        cos_lat = 1e-6
    dlon = radius_km / (111.32 * cos_lat)

    min_lat = clamp(lat - dlat, -90.0, 90.0)
    max_lat = clamp(lat + dlat, -90.0, 90.0)
    min_lon = clamp(lon - dlon, -180.0, 180.0)
    max_lon = clamp(lon + dlon, -180.0, 180.0)
    return (min_lat, min_lon, max_lat, max_lon)


async def ensure_indexes():
    db = app.state.mongodb

    fixed_positions_coll = db["fixedRegionAISstreamiovesselposition"]
    fixed_statics_coll   = db["fixedRegionAISstreamiovesselsstatic"]
    fixed_final_coll     = db["fixedRegionAISstreamiovesselsfinalized"]

    await fixed_positions_coll.create_index("mmsi", unique=True)
    await fixed_statics_coll.create_index("mmsi", unique=True)
    await fixed_final_coll.create_index("mmsi", unique=True)

    logger.info("MongoDB indexes ensured for fixed-region collections.")


async def get_fixed_bbox():
    """
    Fetch latest polygon from AISfixedpolygon.
    The 'coordinates' field is assumed as [[lon,lat], ...] (closed ring optional).
    Returns (min_lat, min_lon, max_lat, max_lon).
    """
    db = app.state.mongodb
    doc = await db["AISfixedpolygon"].find_one(sort=[("_id", -1)])
    if not doc or not doc.get("coordinates"):
        raise HTTPException(500, "AISfixedpolygon not configured")

    coords = doc["coordinates"]  # [[lon,lat], ...]
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return (min(lats), min(lons), max(lats), max(lons))

def polygon_bbox(coords_lonlat):
    """
    coords_lonlat: list of [lon, lat]
    returns bbox as (min_lat, min_lon, max_lat, max_lon)
    """
    lons = [p[0] for p in coords_lonlat]
    lats = [p[1] for p in coords_lonlat]
    return (min(lats), min(lons), max(lats), max(lons))


def point_in_polygon(lon: float, lat: float, poly_lonlat) -> bool:
    """
    Ray casting algorithm.
    poly_lonlat: list of [lon, lat] for OUTER ring (closed or not)
    """
    if not poly_lonlat or len(poly_lonlat) < 3:
        return False

    # ensure closed ring not required; algorithm works either way
    inside = False
    x, y = lon, lat
    n = len(poly_lonlat)

    for i in range(n):
        x1, y1 = poly_lonlat[i]
        x2, y2 = poly_lonlat[(i + 1) % n]

        # check edge crosses the horizontal ray
        if ((y1 > y) != (y2 > y)):
            x_at_y = x1 + (y - y1) * (x2 - x1) / ((y2 - y1) or 1e-12)
            if x_at_y > x:
                inside = not inside

    return inside


def geofence_to_area(port_doc, geofence_doc, fallback_radius_km: float):
    """
    Returns a normalized 'area' dict used by both subscription-box generation and PORT-HIT detection.
    """
    port_id = port_doc.get("_id")
    name = str(port_doc.get("name") or "")

    # key used for overrides and persistence
    key = str(port_doc.get("UNLOCODE") or port_id)

    # Default fallback (circle around port lat/long)
    lat = float(port_doc.get("lat"))
    lon = float(port_doc.get("long"))

    if not geofence_doc or not geofence_doc.get("feature"):
        # fallback to radius around port point
        radius_km = float(fallback_radius_km)
        bbox = bbox_around_point_km(lat, lon, radius_km)
        return {
            "port_id": str(port_id),
            "key": key,
            "name": name,
            "shape": "circle",
            "center_lat": lat,
            "center_lon": lon,
            "radius_m": radius_km * 1000.0,
            "bbox": bbox,
        }

    feat = geofence_doc["feature"]
    props = (feat.get("properties") or {})
    geom = (feat.get("geometry") or {})
    shape = (props.get("shape") or "").lower()
    gtype = (geom.get("type") or "").lower()

    # HYLA example: shape=circle, geometry=Point, properties.radius in meters
    if shape == "circle" and gtype == "point":
        center_lon, center_lat = geom["coordinates"]  # [lon, lat]
        radius_m = float(props.get("radius") or 0.0)
        radius_km = radius_m / 1000.0
        bbox = bbox_around_point_km(float(center_lat), float(center_lon), radius_km)
        return {
            "port_id": str(port_id),
            "key": key,
            "name": name,
            "shape": "circle",
            "center_lat": float(center_lat),
            "center_lon": float(center_lon),
            "radius_m": radius_m,
            "bbox": bbox,
        }

    # Polygon / MultiPolygon geofence
    if gtype in ("polygon", "multipolygon"):
        coords = geom.get("coordinates") or []
        if gtype == "polygon":
            outer = coords[0] if coords else []
        else:
            # MultiPolygon: take first polygon's outer ring by default
            outer = coords[0][0] if coords and coords[0] else []

        bbox = polygon_bbox(outer) if outer else bbox_around_point_km(lat, lon, float(fallback_radius_km))
        return {
            "port_id": str(port_id),
            "key": key,
            "name": name,
            "shape": "polygon",
            "polygon": outer,  # list of [lon, lat]
            "bbox": bbox,
        }

    # Unknown shape -> fallback
    radius_km = float(fallback_radius_km)
    bbox = bbox_around_point_km(lat, lon, radius_km)
    return {
        "port_id": str(port_id),
        "key": key,
        "name": name,
        "shape": "circle",
        "center_lat": lat,
        "center_lon": lon,
        "radius_m": radius_km * 1000.0,
        "bbox": bbox,
    }


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

async def load_port_areas():
    hyla_db = app.state.hyla_db
    ports_coll = hyla_db[settings.HYLA_PORTS_COLLECTION]
    geof_coll = hyla_db["port_regions"]

    ports = await ports_coll.find(
        {"isActive": True},
        {"lat": 1, "long": 1, "UNLOCODE": 1, "name": 1}  # _id comes by default
    ).to_list(None)

    if not ports:
        return []

    # Collect port ObjectIds
    port_ids = [p["_id"] for p in ports if p.get("_id") is not None]

    # Fetch geofences matching portId -> ports._id
    geofences = await geof_coll.find(
        {"portId": {"$in": port_ids}},
        {"portId": 1, "feature": 1}
    ).to_list(None)

    geofence_map = {str(g["portId"]): g for g in geofences if g.get("portId") is not None}

    # Overrides in MAIN DB (enabled/priority, optional radiusKm fallback)
    def port_key(p):
        if p.get("UNLOCODE"):
            return str(p["UNLOCODE"]).strip()
        return str(p.get("_id"))

    keys = [port_key(p) for p in ports if port_key(p)]
    overrides_coll = app.state.mongodb[settings.AIS_PORT_OVERRIDES_COLLECTION]
    overrides = await overrides_coll.find({"key": {"$in": keys}}).to_list(None)
    override_map = {str(o.get("key")): o for o in overrides if o.get("key") is not None}

    enriched = []
    for p in ports:
        k = port_key(p)
        if not k:
            continue

        # must have lat/long in port doc for fallback
        if p.get("lat") is None or p.get("long") is None:
            continue

        o = override_map.get(k, {})
        if o.get("enabled", True) is False:
            continue

        priority = int(o.get("priority", 0))

        # fallback radius only used when geofence missing or unknown
        fallback_radius_km = float(o.get("radiusKm", settings.PORT_RADIUS_KM))

        g = geofence_map.get(str(p["_id"]))  # join on portId
        area = geofence_to_area(p, g, fallback_radius_km)
        area["priority"] = priority

        enriched.append(area)

    enriched.sort(key=lambda x: (-x["priority"], x["name"]))
    return enriched[: settings.PORTS_MAX_BOXES]


async def get_port_bounding_boxes():
    if not settings.USE_PORT_BOXES:
        app.state.port_areas = []
        return []

    areas = await load_port_areas()
    app.state.port_areas = areas  # cache for PORT-HIT detection

    boxes = []
    for a in areas:
        min_lat, min_lon, max_lat, max_lon = a["bbox"]
        boxes.append([[min_lat, min_lon], [max_lat, max_lon]])

    return boxes



async def build_subscription_boxes():
    """
    Build combined BoundingBoxes:
    - fixed polygon bbox (optional)
    - port radius bboxes (optional)
    """
    boxes = []

    if settings.USE_FIXED_POLYGON:
        min_lat, min_lon, max_lat, max_lon = await get_fixed_bbox()
        boxes.append([[min_lat, min_lon], [max_lat, max_lon]])

    port_boxes = await get_port_bounding_boxes()
    boxes.extend(port_boxes)

    return boxes


async def connect_with_retries(url, retries=5, delay=3):
    attempt = 0
    backoff = delay
    while True:
        try:
            logger.info(f"Connecting to AIS stream (attempt {attempt + 1})...")
            ws = await websockets.connect(
                url,
                ping_interval=None,   # app-managed (disabled internal)
                ping_timeout=None,
            )
            logger.info("Connected to AIS stream.")
            return ws
        except Exception as e:
            attempt += 1
            if attempt >= retries:
                logger.warning(
                    f"Failed to connect after {retries} attempts: {e}. "
                    f"Will keep retrying every {backoff} seconds."
                )
                attempt = 0
            else:
                logger.warning(f"Connection failed: {e}. Retrying in {backoff} seconds...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def subscription_refresh_loop(ws, userkey: str):
    """
    Periodically rebuild polygon+ports boxes and re-send subscription on the SAME ws.
    Never create a new websocket here.
    """
    last_boxes = None

    while True:
        await asyncio.sleep(settings.SUBSCRIPTION_REFRESH_SECONDS)

        try:
            boxes = await build_subscription_boxes()
            if not boxes:
                logger.warning("Subscription refresh: no boxes built; skipping this cycle.")
                continue

            if boxes == last_boxes:
                continue

            payload = {
                "APIKey": userkey,
                "BoundingBoxes": boxes,
                "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
            }

            await ws.send(json.dumps(payload))
            last_boxes = boxes

            logger.info(
                f"Subscription refreshed: total_boxes={len(boxes)} "
                f"(polygon={'on' if settings.USE_FIXED_POLYGON else 'off'}, "
                f"ports={'on' if settings.USE_PORT_BOXES else 'off'})"
            )

        except websockets.ConnectionClosed:
            logger.info("Subscription refresh loop: websocket closed. Exiting refresh loop.")
            break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Subscription refresh loop error: {e}")

# -------------------------------------------------------------------
# Merge logic
# -------------------------------------------------------------------

async def merge_fixed_region_vessels():
    db = app.state.mongodb
    pos_coll = db["fixedRegionAISstreamiovesselposition"]

    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    since = now - timedelta(minutes=max(THRESHOLDS))

    recent = await pos_coll.find(
        {"timestamp": {"$gte": since.isoformat()}},
        {"mmsi": 1}
    ).to_list(None)

    for p in recent:
        mmsi = p.get("mmsi")
        if mmsi:
            enqueue_merge(mmsi)

    logger.info("merge_fixed_region_vessels (latest-state) run completed.")


# async def merge_latest_for_mmsi(mmsi: str):
#     db = app.state.mongodb

#     pos_coll     = db["fixedRegionAISstreamiovesselposition"]
#     statics_coll = db["fixedRegionAISstreamiovesselsstatic"]
#     final_coll   = db["fixedRegionAISstreamiovesselsfinalized"]

#     # single doc per MMSI due to unique index, but sort defensively
#     pos = await pos_coll.find_one({"mmsi": mmsi})
#     if not pos:
#         return

#     ts = pos.get("timestamp")
#     pos_time = parse_iso(ts)
#     if not pos_time:
#         return

#     now = datetime.utcnow().replace(tzinfo=timezone.utc)
#     since = now - timedelta(minutes=max(THRESHOLDS))
#     if pos_time < since:
#         return

#     static_candidates = (
#         await statics_coll.find({"mmsi": mmsi})
#         .sort("timestamp", -1)
#         .limit(5)
#         .to_list(None)
#     )

#     static_doc = None
#     static_t = None
#     diff_min = None

#     for st in static_candidates:
#         st_ts = st.get("timestamp")
#         static_time = parse_iso(st_ts)
#         if not static_time:
#             continue

#         diff_min = abs((pos_time - static_time).total_seconds()) / 60.0
#         t = next((thr for thr in THRESHOLDS if diff_min <= thr), None)
#         if t is not None:
#             static_doc = st
#             static_t = t
#             break

#     if not static_doc:
#         return

#     await final_coll.update_one(
#         {"mmsi": mmsi},
#         {
#             "$set": {
#                 "merged_at": datetime.now(timezone.utc).isoformat(),
#                 "threshold_minutes": static_t,
#                 "threshold_flag": f"≤{static_t}m" if static_t else None,
#                 "time_diff_minutes": diff_min,
#                 "timestamp_position": pos["timestamp"],
#                 "timestamp_static": static_doc.get("timestamp"),
#                 "position_data": pos,
#                 "static_data": static_doc,
#             }
#         },
#         upsert=True,
#     )


# commented above to add below on 05-02-2025 (fixed-latest position was not updating in finalized due to threshold)
# async def merge_latest_for_mmsi(mmsi: str):
#     db = app.state.mongodb

#     try:
#         mmsi_int = int(mmsi)
#     except Exception:
#         return
    

#     pos_coll     = db["fixedRegionAISstreamiovesselposition"]
#     statics_coll = db["fixedRegionAISstreamiovesselsstatic"]
#     final_coll   = db["fixedRegionAISstreamiovesselsfinalized"]

#     pos = await pos_coll.find_one({"mmsi": {"$in": [mmsi_int, str(mmsi_int)]}})
#     st  = await statics_coll.find_one({"mmsi": {"$in": [mmsi_int, str(mmsi_int)]}})


#     # If neither exists, nothing to do
#     if not pos and not st:
#         return

#     now_iso = datetime.now(timezone.utc).isoformat()

#     # Prepare base update: always keep latest position/static if present
#     update_doc = {
#         "updated_at": now_iso,
#         "mmsi": mmsi_int,
#     }

#     if pos:
#         update_doc["timestamp_position"] = pos.get("timestamp")
#         update_doc["position_data"] = pos

#     if st:
#         update_doc["timestamp_static"] = st.get("timestamp")
#         update_doc["static_data"] = st

#     # Compute diff + threshold flags if both present + parseable
#     diff_min = None
#     threshold_t = None
#     merge_ok = False

#     if pos and st:
#         pos_time = parse_iso(pos.get("timestamp"))
#         st_time  = parse_iso(st.get("timestamp"))

#         if pos_time and st_time:
#             diff_min = abs((pos_time - st_time).total_seconds()) / 60.0
#             threshold_t = next((thr for thr in THRESHOLDS if diff_min <= thr), None)
#             merge_ok = threshold_t is not None

#     update_doc["time_diff_minutes"] = diff_min
#     update_doc["threshold_minutes"] = threshold_t
#     update_doc["threshold_flag"] = (f"≤{threshold_t}m" if threshold_t else None)
#     update_doc["merge_ok"] = merge_ok
#     update_doc["merged_at"] = (now_iso if merge_ok else None)  # optional meaning: last time merge was valid

#     await final_coll.update_one(
#         {"mmsi": mmsi_int},
#         {"$set": update_doc},
#         upsert=True,
#     )


async def merge_latest_for_mmsi(mmsi: str):
    db = app.state.mongodb

    try:
        mmsi_int = int(mmsi)
    except Exception:
        return

    pos_coll     = db["fixedRegionAISstreamiovesselposition"]
    statics_coll = db["fixedRegionAISstreamiovesselsstatic"]
    final_coll   = db["fixedRegionAISstreamiovesselsfinalized"]

    # ✅ read current finalized (for drift detection)
    existing_final = await final_coll.find_one({"mmsi": mmsi_int}, {"timestamp_position": 1, "timestamp_static": 1})

    pos = await pos_coll.find({"mmsi": {"$in": [mmsi_int, str(mmsi_int)]}}) \
        .sort("timestamp", -1).limit(1).to_list(1)
    pos = pos[0] if pos else None

    st = await statics_coll.find({"mmsi": {"$in": [mmsi_int, str(mmsi_int)]}}) \
        .sort("timestamp", -1).limit(1).to_list(1)
    st = st[0] if st else None

    logger.info(
    "MERGE READ mmsi=%s pos_ts=%s static_ts=%s",
    mmsi_int,
    pos.get("timestamp") if pos else None,
    st.get("timestamp") if st else None,
    )

    if not pos and not st:
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    # ✅ DRIFT LOGS
    try:
        if existing_final and pos:
            old_tp = existing_final.get("timestamp_position")
            new_tp = pos.get("timestamp")
            if old_tp != new_tp:
                logger.info(
                    "FINALIZED POSITION UPDATE mmsi=%s old=%s new=%s",
                    mmsi_int, old_tp, new_tp
                )
        if existing_final and st:
            old_ts = existing_final.get("timestamp_static")
            new_ts = st.get("timestamp")
            if old_ts != new_ts:
                logger.info(
                    "FINALIZED STATIC UPDATE mmsi=%s old=%s new=%s",
                    mmsi_int, old_ts, new_ts
                )
    except Exception:
        logger.exception("Drift logging failed for mmsi=%s", mmsi_int)

    update_doc = {
        "updated_at": now_iso,
        "mmsi": mmsi_int,
    }

    if pos:
        update_doc["timestamp_position"] = pos.get("timestamp")
        update_doc["position_data"] = pos

    if st:
        update_doc["timestamp_static"] = st.get("timestamp")
        update_doc["static_data"] = st

    diff_min = None
    threshold_t = None
    merge_ok = False

    if pos and st:
        pos_time = parse_iso(pos.get("timestamp"))
        st_time  = parse_iso(st.get("timestamp"))
        if pos_time and st_time:
            diff_min = abs((pos_time - st_time).total_seconds()) / 60.0
            threshold_t = next((thr for thr in THRESHOLDS if diff_min <= thr), None)
            merge_ok = threshold_t is not None

    update_doc["time_diff_minutes"] = diff_min
    update_doc["threshold_minutes"] = threshold_t
    update_doc["threshold_flag"] = (f"≤{threshold_t}m" if threshold_t else None)
    update_doc["merge_ok"] = merge_ok
    update_doc["merged_at"] = (now_iso if merge_ok else None)

    res = await final_coll.update_one(
        {"mmsi": mmsi_int},
        {"$set": update_doc},
        upsert=True,
    )

    # ✅ confirm write happened
    logger.info(
        "FINALIZED UPSERT mmsi=%s matched=%s modified=%s upserted_id=%s",
        mmsi_int, res.matched_count, res.modified_count, str(res.upserted_id) if res.upserted_id else None
    )


# -------------------------------------------------------------------
# Merge queue (dedupe) to avoid create_task storm
# -------------------------------------------------------------------
# start-on-05-02-26
async def merge_queue_worker(worker_id: int = 0):
    """
    Single background consumer that processes merge_latest_for_mmsi(mmsi)
    from a queue. Prevents spawning 50k tasks.
    """
    while True:
        mmsi_key = await app.state.merge_q.get()
        try:
            logger.debug("[merge_worker %s] processing mmsi=%s", worker_id, mmsi_key)
            await merge_latest_for_mmsi(mmsi_key)
        except Exception:
            logger.exception(f"[merge_worker {worker_id}] merge_latest_for_mmsi failed for MMSI={mmsi_key}")
        finally:
            # allow MMSI to be queued again later
            app.state.merge_pending.discard(mmsi_key)
            app.state.merge_q.task_done()
# end

def enqueue_merge(mmsi):
    if mmsi is None:
        return

    try:
        mmsi_key = str(int(mmsi))
    except Exception:
        mmsi_key = str(mmsi).strip()
        if not mmsi_key:
            return

    if not hasattr(app.state, "merge_pending") or not hasattr(app.state, "merge_q"):
        return

    # already enqueued / processing
    if mmsi_key in app.state.merge_pending:
        return

    try:
        app.state.merge_q.put_nowait(mmsi_key)
        app.state.merge_pending.add(mmsi_key)
    except asyncio.QueueFull:
        # queue full -> retry later via overflow
        if not hasattr(app.state, "merge_overflow"):
            app.state.merge_overflow = set()
        app.state.merge_overflow.add(mmsi_key)
        logger.warning("merge queue full; staged in overflow for mmsi=%s", mmsi_key)


async def merge_loop_worker():
    """
    Background loop:
    - Run merge_fixed_region_vessels()
    - Wait 10 minutes
    - Repeat
    """
    while True:
        try:
            await merge_fixed_region_vessels()
        except Exception as e:
            logger.exception("Error in merge loop worker: %s", e)

        await asyncio.sleep(10 * 60)


# -------------------------------------------------------------------
# AIS background worker
# -------------------------------------------------------------------

async def ais_fixed_region_worker():
    db = app.state.mongodb
    positions_coll = db["fixedRegionAISstreamiovesselposition"]
    statics_coll   = db["fixedRegionAISstreamiovesselsstatic"]

    api_key_doc = await db["maritimeapikey"].find_one({"source": settings.AIS_API_SOURCE})
    if not api_key_doc:
        logger.error("aisstreamio API key not configured in maritimeapikey collection.")
    userkey = api_key_doc["key"] if api_key_doc else None

    while True:
        try:
            if not userkey:
                api_key_doc = await db["maritimeapikey"].find_one({"source": settings.AIS_API_SOURCE})
                if not api_key_doc:
                    logger.error("Still no API key in maritimeapikey. Waiting 30 seconds...")
                    await asyncio.sleep(30)
                    continue
                userkey = api_key_doc["key"]

            # Build combined subscription: polygon bbox + port bboxes
            boxes = await build_subscription_boxes()
            if not boxes:
                logger.error("No bounding boxes available (polygon+ports). Waiting 30 seconds...")
                await asyncio.sleep(30)
                continue
            ws = await connect_with_retries(AISSTREAM_URL)

            

            subscribe_payload = {
                "APIKey": userkey,
                "BoundingBoxes": boxes,
                "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
            }

            # Must be sent quickly after connect (AISStream requirement)
            await ws.send(json.dumps(subscribe_payload))
            logger.info(
                f"Subscription sent to AIS stream. total_boxes={len(boxes)} "
                f"(polygon={'on' if settings.USE_FIXED_POLYGON else 'off'}, ports={'on' if settings.USE_PORT_BOXES else 'off'})"
            )

            # Start subscription refresher (ports/polygon can change over time)
            refresh_task = asyncio.create_task(subscription_refresh_loop(ws, userkey))

            # main receive loop
            while True:
                try:
                    raw = await ws.recv()
                except websockets.ConnectionClosed as e:
                    logger.warning(f"Upstream WebSocket closed: {e}. Reconnecting...")
                    break
                except asyncio.CancelledError:
                    logger.info("AIS worker cancelled. Closing upstream connection.")
                    raise

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("Received invalid JSON from AIS stream.")
                    continue

                message_type = msg.get("MessageType")
                meta = msg.get("MetaData", {}) or {}
                now_iso = datetime.now(timezone.utc).isoformat()

                if message_type == "PositionReport":
                    report = (msg.get("Message") or {}).get("PositionReport") or {}
                    if not report:
                        continue

                    mmsi = report.get("UserID") or meta.get("MMSI")
                    lat = report.get("Latitude") or report.get("latitude")
                    lon = report.get("Longitude") or report.get("longitude")

                    if not mmsi or lat is None or lon is None:
                        continue

                    # ✅ PUT THE PORT-HIT BLOCK RIGHT HERE (exactly here)
                    # --- PORT-HIT confirmation (inference) ---
                    try:
                        areas = getattr(app.state, "port_areas", []) or []
                        now_ts = datetime.now(timezone.utc).timestamp()

                        vlat = float(lat)
                        vlon = float(lon)

                        for a in areas:
                            min_lat, min_lon, max_lat, max_lon = a["bbox"]

                            # quick bbox reject
                            if not (min_lat <= vlat <= max_lat and min_lon <= vlon <= max_lon):
                                continue

                            # dist = haversine_km(vlat, vlon, a["lat"], a["lon"])
                            dist_km = None
                            inside = False

                            # circle geofence
                            if a.get("shape") == "circle":
                                dist_km = haversine_km(vlat, vlon, a["center_lat"], a["center_lon"])
                                inside = dist_km <= (float(a["radius_m"]) / 1000.0)

                            # polygon geofence
                            elif a.get("shape") == "polygon":
                                inside = point_in_polygon(vlon, vlat, a.get("polygon") or [])

                            if inside:
                                key = f"{mmsi}:{a['key']}"
                                last = app.state.port_hit_last.get(key, 0)

                                if now_ts - last >= 60:
                                    app.state.port_hit_last[key] = now_ts
                                    # if dist_km is not None:
                                    #     print(f"PORT-HIT mmsi={mmsi} port={a['name']}({a['key']}) dist_km={dist_km:.2f} shape=circle")
                                    # else:
                                    #     print(f"PORT-HIT mmsi={mmsi} port={a['name']}({a['key']}) shape=polygon")

                                # Persist hit (keep your current $inc-only pattern)
                                hits = app.state.mongodb["ais_port_hits"]
                                now_dt = datetime.utcnow()

                                await hits.update_one(
                                    {"mmsi": str(mmsi), "port_key": str(a["key"])},
                                    {
                                        "$setOnInsert": {"first_seen": now_dt},
                                        "$set": {
                                            "last_seen": now_dt,
                                            "port_id": a.get("port_id"),
                                            "port_name": a["name"],
                                            "shape": a.get("shape"),
                                            "radius_m": float(a["radius_m"]) if a.get("shape") == "circle" else None,
                                            "dist_km": float(dist_km) if dist_km is not None else None,
                                            "vessel_location": {"type": "Point", "coordinates": [vlon, vlat]},
                                            "expiresAt": now_dt + timedelta(days=7),
                                        },
                                        "$inc": {"hit_count": 1},
                                    },
                                    upsert=True,
                                )
                                break

                    #         if dist <= a["radius_km"]:
                    #             key = f"{mmsi}:{a['key']}"
                    #             last = app.state.port_hit_last.get(key, 0)

                    #             # rate limit: once per 60 seconds per (mmsi,port)
                    #             if now_ts - last >= 60:
                    #                 app.state.port_hit_last[key] = now_ts
                    #                 print(
                    #                     f"PORT-HIT mmsi={mmsi} port={a['name']}({a['key']}) "
                    #                     f"dist_km={dist:.2f} radius_km={a['radius_km']}"
                    #                 )
                    #             # Persist hit
                    #             hits = app.state.mongodb["ais_port_hits"]
                    #             now_dt = datetime.utcnow()


                    #             await hits.update_one(
                    #                 {"mmsi": str(mmsi), "port_key": str(a["key"])},
                    #                 {
                    #                     "$setOnInsert": {
                    #                         "first_seen": now_dt,
                    #                     },
                    #                     "$set": {
                    #                         "last_seen": now_dt,
                    #                         "port_name": a["name"],
                    #                         "radius_km": float(a["radius_km"]),
                    #                         "dist_km": float(dist),
                    #                         "vessel_location": {"type": "Point", "coordinates": [vlon, vlat]},
                    #                         "expiresAt": now_dt + timedelta(days=7),
                    #                     },
                    #                     "$inc": {"hit_count": 1},
                    #                 },
                    #                 upsert=True,
                    #             )

                    #             break
                    except Exception as e:
                        logger.exception("PORT-HIT detect/persist failed")

                    # then proceed with your existing Mongo write
                    doc = {
                        "timestamp": now_iso,
                        "type": "PositionReport",
                        "mmsi": mmsi,
                        "time_utc": meta.get("time_utc"),
                        "metadata": meta,
                        "location": {"type": "Point", "coordinates": [lon, lat]},
                        "raw_report": report,
                    }

                    try:
                        mmsi_int = int(mmsi)
                    except Exception:
                        continue

                    doc["mmsi"] = mmsi_int

                    await positions_coll.update_one(
                        {"mmsi": mmsi_int},
                        {"$set": doc},
                        upsert=True,
                    )

                    # asyncio.create_task(merge_latest_for_mmsi(mmsi))
                    enqueue_merge(mmsi_int)


                elif message_type == "ShipStaticData":
                    report = (msg.get("Message") or {}).get("ShipStaticData") or {}
                    if not report:
                        continue

                    mmsi = report.get("UserID") or meta.get("MMSI")
                    if not mmsi:
                        continue

                    static_doc = {
                        "timestamp": now_iso,
                        "type": "ShipStaticData",
                        "mmsi": mmsi,
                        "time_utc": meta.get("time_utc"),
                        "metadata": meta,
                        "raw_report": report,
                    }

                    await statics_coll.update_one(
                        {"mmsi": mmsi},
                        {"$set": static_doc},
                        upsert=True,
                    )

                    try:
                        # asyncio.create_task(merge_latest_for_mmsi(mmsi))
                        enqueue_merge(mmsi)

                    except Exception as e:
                        logger.exception(f"Immediate merge failed for MMSI={mmsi}: {e}")

            # connection ended -> stop refresh task
            if refresh_task:
                refresh_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await refresh_task

        except asyncio.CancelledError:
            logger.info("AIS fixed-region worker task cancelled. Exiting loop.")
            break
        except Exception as e:
            logger.exception(f"Stream-level error in AIS worker: {e}")
            await asyncio.sleep(2)


# -------------------------------------------------------------------
# FastAPI lifecycle
# -------------------------------------------------------------------


async def merge_overflow_flusher():
    while True:
        await asyncio.sleep(0.2)

        q = app.state.merge_q
        overflow = app.state.merge_overflow
        pending = app.state.merge_pending

        if not overflow or q.full():
            continue

        for mmsi_key in list(overflow):
            if q.full():
                break

            # only enqueue if not already pending (extra safety)
            if mmsi_key in pending:
                overflow.discard(mmsi_key)
                continue

            try:
                q.put_nowait(mmsi_key)
                pending.add(mmsi_key)
                overflow.discard(mmsi_key)
            except asyncio.QueueFull:
                break


@app.on_event("startup")
async def startup_event():
    # connect Mongo (main)
    try:
        setup_file_logging()
        logger.info("=== AIS ingestor starting (code version: 2026-02-06) ===")
        app.state.mongodb_client = AsyncIOMotorClient(settings.MONGO_URI)
        db_name = app.state.mongodb_client.get_default_database().name
        app.state.mongodb = app.state.mongodb_client[db_name]
        logger.info(f"Connected to MAIN MongoDB database '{db_name}'.")

        app.state.merge_q = asyncio.Queue(maxsize=settings.MERGE_QUEUE_MAX)
        app.state.merge_pending = set()
        app.state.merge_overflow = set()

        #start flusher After structures exist
        app.state.merge_flusher_task = asyncio.create_task(merge_overflow_flusher())

        # start workers
        app.state.merge_q_workers = []
        for i in range(settings.MERGE_WORKERS):
            t = asyncio.create_task(merge_queue_worker(i))
            app.state.merge_q_workers.append(t)
        logger.info(f"Merge queue workers started: {settings.MERGE_WORKERS}")

    except Exception as e:
        logger.error(f"Failed to connect to MAIN MongoDB: {e}")
        raise RuntimeError(f"Failed to connect to MAIN MongoDB: {e}")

    # --- Port hit persistence indexes ---
    hits = app.state.mongodb["ais_port_hits"]
    await hits.create_index([("mmsi", 1), ("port_key", 1)], unique=True)
    await hits.create_index("last_seen")
    # optional TTL: keep only 7 days
    await hits.create_index("expiresAt", expireAfterSeconds=0)

    # connect Mongo (Hyla)
    try:
        app.state.hyla_client = AsyncIOMotorClient(settings.HYLA_MONGO_URI)
        hyla_db_name = app.state.hyla_client.get_default_database().name
        app.state.hyla_db = app.state.hyla_client[hyla_db_name]
        app.state.port_areas = []
        app.state.port_hit_last = {}  # rate-limit logging per (mmsi, port)

        logger.info(f"Connected to HYLA MongoDB database '{hyla_db_name}'.")
    except Exception as e:
        logger.error(f"Failed to connect to HYLA MongoDB: {e}")
        raise RuntimeError(f"Failed to connect to HYLA MongoDB: {e}")

    # indexes
    await ensure_indexes()

    # start merge loop worker
    app.state.merge_task = asyncio.create_task(merge_loop_worker())
    logger.info("Merge loop worker started (runs, then waits 10 min, then runs again).")

    # start AIS worker
    app.state.ais_task = asyncio.create_task(ais_fixed_region_worker())
    logger.info("AIS fixed-region background worker started.")



@app.on_event("shutdown")
async def shutdown_event():
    # cancel merge loop task
    merge_task = getattr(app.state, "merge_task", None)
    if merge_task:
        merge_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await merge_task
        logger.info("Merge loop worker task cancelled.")

    # cancel AIS worker task
    ais_task = getattr(app.state, "ais_task", None)
    if ais_task:
        ais_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ais_task
        logger.info("AIS worker task cancelled.")

    # start-on-05-02-26
    # cancel merge queue worker
    workers = getattr(app.state, "merge_q_workers", [])
    for t in workers:
        t.cancel()
    for t in workers:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    logger.info("Merge queue worker tasks cancelled.")

    flusher = getattr(app.state, "merge_flusher_task", None)
    if flusher:
        flusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await flusher


    # end

    # close Mongo clients
    # close Mongo clients (do not crash on shutdown)
    client = getattr(app.state, "mongodb_client", None)
    if client:
        try:
            client.close()
            logger.info("MAIN MongoDB client closed.")
        except Exception:
            logger.warning("MAIN MongoDB client close failed (ignored).", exc_info=True)

    hyla_client = getattr(app.state, "hyla_client", None)
    if hyla_client:
        try:
            hyla_client.close()
            logger.info("HYLA MongoDB client closed.")
        except Exception:
            logger.warning("HYLA MongoDB client close failed (ignored).", exc_info=True)




# -------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/debug/ingest-status")
async def ingest_status():
    db = app.state.mongodb
    pos_count = await db["fixedRegionAISstreamiovesselposition"].count_documents({})
    static_count = await db["fixedRegionAISstreamiovesselsstatic"].count_documents({})
    final_count = await db["fixedRegionAISstreamiovesselsfinalized"].count_documents({})
    return {
        "position_count": pos_count,
        "static_count": static_count,
        "finalized_count": final_count,
    }


@app.get("/debug/subscription-boxes")
async def debug_subscription_boxes():
    boxes = await build_subscription_boxes()
    return {
        "total_boxes": len(boxes),
        "use_fixed_polygon": settings.USE_FIXED_POLYGON,
        "use_port_boxes": settings.USE_PORT_BOXES,
        "port_radius_km": settings.PORT_RADIUS_KM,
        "ports_max_boxes": settings.PORTS_MAX_BOXES,
        "boxes_preview": boxes[:5],
    }

from fastapi import Query

@app.get("/debug/port-hit-vessels")
async def port_hit_vessels(minutes: int = Query(60, ge=1, le=10080), limit: int = Query(200, ge=1, le=2000)):
    """
    Returns vessels that had a PORT-HIT within the last N minutes.
    """
    db = app.state.mongodb
    hits = db["ais_port_hits"]
    since = datetime.utcnow() - timedelta(minutes=minutes)


    cursor = hits.find({"last_seen": {"$gte": since}}).sort("last_seen", -1).limit(limit)
    rows = await cursor.to_list(None)

    # Convert ObjectId / datetime to JSON-friendly
    out = []
    for r in rows:
        r["_id"] = str(r.get("_id"))
        if isinstance(r.get("first_seen"), datetime):
            r["first_seen"] = r["first_seen"].isoformat()
        if isinstance(r.get("last_seen"), datetime):
            r["last_seen"] = r["last_seen"].isoformat()
        if isinstance(r.get("expiresAt"), datetime):
            r["expiresAt"] = r["expiresAt"].isoformat()
        out.append(r)

    return {"since": since.isoformat(), "count": len(out), "items": out}


@app.get("/debug/port-hit-summary")
async def port_hit_summary(minutes: int = Query(60, ge=1, le=10080)):
    """
    Aggregate hits per port for the last N minutes.
    """
    db = app.state.mongodb
    hits = db["ais_port_hits"]
    since = datetime.utcnow() - timedelta(minutes=minutes)

    pipeline = [
        {"$match": {"last_seen": {"$gte": since}}},
        {"$group": {"_id": {"port_key": "$port_key", "port_name": "$port_name"},
                    "vessels": {"$addToSet": "$mmsi"},
                    "total_hits": {"$sum": "$hit_count"}}},
        {"$project": {"_id": 0,
                      "port_key": "$_id.port_key",
                      "port_name": "$_id.port_name",
                      "unique_vessels": {"$size": "$vessels"},
                      "total_hits": 1}},
        {"$sort": {"unique_vessels": -1, "total_hits": -1}},
    ]

    rows = await hits.aggregate(pipeline).to_list(None)
    return {"since": since.isoformat(), "ports": rows}

@app.get("/debug/port-hit-raw")
async def port_hit_raw(limit: int = 50):
    hits = app.state.mongodb["ais_port_hits"]
    rows = await hits.find({}).sort("_id", -1).limit(limit).to_list(None)

    out = []
    for r in rows:
        r["_id"] = str(r.get("_id"))
        for k in ("first_seen", "last_seen", "expiresAt"):
            if isinstance(r.get(k), datetime):
                r[k] = r[k].isoformat()
        out.append(r)

    return {"count": await hits.count_documents({}), "items": out}

@app.get("/debug/port-areas")
async def debug_port_areas():
    areas = getattr(app.state, "port_areas", []) or []
    return {
        "count": len(areas),
        "items": [
            {
                "key": a["key"],
                "port_id": a.get("port_id"),
                "name": a["name"],
                "shape": a.get("shape"),
                "priority": a.get("priority", 0),
                "bbox": a.get("bbox"),
            }
            for a in areas
        ],
    }

@app.get("/debug/mmsi/{mmsi}")
async def debug_mmsi(mmsi: int):
    db = app.state.mongodb
    pos = await db["fixedRegionAISstreamiovesselposition"].find_one({"mmsi": {"$in": [mmsi, str(mmsi)]}})
    fin = await db["fixedRegionAISstreamiovesselsfinalized"].find_one({"mmsi": mmsi})

    def pick(d):
        if not d: return None
        return {
            "timestamp": d.get("timestamp"),
            "time_utc": d.get("time_utc"),
            "location": d.get("location"),
        }

    return {
        "mmsi": mmsi,
        "position_latest": pick(pos),
        "finalized_timestamp_position": fin.get("timestamp_position") if fin else None,
        "finalized_updated_at": fin.get("updated_at") if fin else None,
        "finalized_merge_ok": fin.get("merge_ok") if fin else None,
    }


@app.get("/debug/finalized-drift")
async def finalized_drift(limit: int = 200):
    db = app.state.mongodb
    pos = db["fixedRegionAISstreamiovesselposition"]

    pipeline = [
        {"$lookup": {
            "from": "fixedRegionAISstreamiovesselsfinalized",
            "let": {"m": "$mmsi"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$mmsi", "$$m"]}}},
                {"$sort": {"updated_at": -1, "_id": -1}},
                {"$limit": 1},
                {"$project": {"timestamp_position": 1, "updated_at": 1}}
            ],
            "as": "fin"
        }},
        {"$unwind": {"path": "$fin", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "mmsi": 1,
            "pos_ts": "$timestamp",
            "fin_ts": "$fin.timestamp_position",
            "fin_updated_at": "$fin.updated_at",
        }},
        {"$match": {"$expr": {"$ne": ["$pos_ts", "$fin_ts"]}}},
        {"$sort": {"pos_ts": -1}},
        {"$limit": limit},
    ]
    rows = await pos.aggregate(pipeline).to_list(None)
    return {"count": len(rows), "items": rows}

@app.post("/admin/backfill-finalized-from-positions")
async def backfill_finalized_from_positions(limit: int = 5000):
    """
    One-time backfill:
    For each MMSI in positions, ensure finalized.timestamp_position == positions.timestamp
    by enqueueing merges in bulk.
    """
    db = app.state.mongodb
    pos = db["fixedRegionAISstreamiovesselposition"]

    # Take latest positions (by timestamp) and enqueue
    cursor = pos.find({}, {"mmsi": 1}).limit(limit)
    rows = await cursor.to_list(None)

    n = 0
    for r in rows:
        m = r.get("mmsi")
        if m is None:
            continue
        enqueue_merge(m)
        n += 1

    return {"enqueued": n, "limit": limit}

@app.post("/admin/enqueue/{mmsi}")
async def admin_enqueue(mmsi: int):
    enqueue_merge(mmsi)
    return {"enqueued": str(mmsi)}

@app.get("/debug/merge-queue")
async def debug_merge_queue():
    q = app.state.merge_q
    return {
        "qsize": q.qsize(),
        "pending": len(app.state.merge_pending),
        "overflow": len(app.state.merge_overflow),
        "workers": len(getattr(app.state, "merge_q_workers", [])),
    }
