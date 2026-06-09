from dotenv import load_dotenv
load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

from apps.orders.knowledge_base import get_staffing_manager_for_client
print("Staffing Manager returned by KB:", get_staffing_manager_for_client(1618))
