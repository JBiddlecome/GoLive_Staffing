import os
from dotenv import load_dotenv
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from apps.orders.knowledge_base import get_staffing_managers_for_clients

print("Testing get_staffing_managers_for_clients...")
try:
    res, success = get_staffing_managers_for_clients([63])
    print("Result:", res)
    print("Success:", success)
except Exception as e:
    import traceback
    traceback.print_exc()
