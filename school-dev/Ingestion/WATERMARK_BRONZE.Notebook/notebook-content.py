# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bfe479b8-2f70-44bc-84d5-dfa2ec50d321",
# META       "default_lakehouse_name": "LH_BRONZE",
# META       "default_lakehouse_workspace_id": "28e6a84a-1953-410e-8b52-272e6318afde",
# META       "known_lakehouses": [
# META         {
# META           "id": "bfe479b8-2f70-44bc-84d5-dfa2ec50d321"
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
