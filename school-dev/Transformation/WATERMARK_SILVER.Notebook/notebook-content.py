# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5b11fd41-c6a9-48d3-968c-c2b8633746e8",
# META       "default_lakehouse_name": "LH_SILVER",
# META       "default_lakehouse_workspace_id": "ca671cc1-0874-4ef0-aac1-5c341115234f",
# META       "known_lakehouses": [
# META         {
# META           "id": "5b11fd41-c6a9-48d3-968c-c2b8633746e8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import json


now_et   = datetime.now(ZoneInfo("America/New_York"))
now_utc  = now_et.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

folder = "Files/Watermarks"
file   = f"{folder}/watermark.json"

mssparkutils.fs.mkdirs(folder)  
mssparkutils.fs.put(file, json.dumps({"lastModified": now_utc}, indent=2), overwrite=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
