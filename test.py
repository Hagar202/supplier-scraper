import requests
import time

BASE_URL = "http://127.0.0.1:5000"

# 1️⃣ بدء scan جديد بالـ data الصح
data = {"commodity": "groceries", "state": "California", "max_results": 5}  # قيم مناسبة للسيرفر
response = requests.post(f"{BASE_URL}/api/start_scan", json=data)
start_scan_result = response.json()
print("Start Scan Response:", start_scan_result)

# احصل على scan_id
scan_id = start_scan_result.get("scan_id")
if not scan_id:
    print("Failed to start scan")
    exit()

# 2️⃣ متابعة التقدم
time.sleep(2)  # استنى شوية قبل أول طلب متابعة
status_response = requests.get(f"{BASE_URL}/api/scan_status/{scan_id}")
print("Scan Status:", status_response.json())

# 3️⃣ فحص صحة السيرفر
health = requests.get(f"{BASE_URL}/api/health")
print("Health Check:", health.json())

# 4️⃣ تصدير البيانات بصيغة JSON لو السكـان خلص
time.sleep(5)  # ممكن تغيري حسب سرعة السيرفر
export_response = requests.get(f"{BASE_URL}/api/export/{scan_id}/json")
if export_response.status_code == 200:
    print("Exported Data (JSON):", export_response.json())
else:
    print("Export Failed:", export_response.json())
