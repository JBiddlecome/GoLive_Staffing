import requests
import os

def test_upload():
    url = "http://127.0.0.1:8181/pay-rate-reduction-calculator/upload-spreadsheet"
    csv_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\apps\pay_rate_reduction_calculator\Markup_Analysis_Option_A.csv"
    
    if not os.path.exists(csv_path):
        print(f"Error: csv file not found at {csv_path}")
        return
        
    print(f"Uploading {csv_path} to {url}...")
    try:
        with open(csv_path, 'rb') as f:
            files = {'file': (os.path.basename(csv_path), f, 'text/csv')}
            response = requests.post(url, files=files)
            
        print(f"Status Code: {response.status_code}")
        print("Response JSON:")
        print(response.json())
    except Exception as e:
        print("Connection failed. Make sure the uvicorn server is running on port 8000:", e)

if __name__ == "__main__":
    test_upload()
