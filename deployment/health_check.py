#!/usr/bin/env python
import os
import sys
from supabase import create_client

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: Missing Supabase credentials")
    sys.exit(1)

try:
    client = create_client(url, key)
    response = (
        client.table("finn_codes").select("count", count="exact").limit(1).execute()
    )
    print("Healthcheck passed - connected to Supabase")
    sys.exit(0)
except Exception as e:
    print(f"Healthcheck failed: {e}")
    sys.exit(1)
