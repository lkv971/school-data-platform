# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1d3872cf-5662-4218-8fb2-0cac4a7f5491",
# META       "default_lakehouse_name": "LH_BRONZE",
# META       "default_lakehouse_workspace_id": "ca671cc1-0874-4ef0-aac1-5c341115234f",
# META       "known_lakehouses": [
# META         {
# META           "id": "1d3872cf-5662-4218-8fb2-0cac4a7f5491"
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
