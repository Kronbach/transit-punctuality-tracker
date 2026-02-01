import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import unquote, urljoin
import time

base_url = "https://rtec.md"
download_path = "Z:/public_transport/routes"

# Step 1: Get all route page URLs
route_pages = []

for page_num in range(1, 8):
    list_url = f"https://rtec.md/category/transport/page/{page_num}/"
    print(f"Scanning: {list_url}")
    response = requests.get(list_url)
    soup = BeautifulSoup(response.content, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/ruta-nr-" in href and href not in route_pages:
            route_pages.append(href)

    time.sleep(0.5)

print(f"Found {len(route_pages)} route pages")

# Step 2: Download PDFs from each route page
os.makedirs(download_path, exist_ok=True)

for route_url in route_pages:
    print(f"Processing: {route_url}")
    response = requests.get(route_url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Find links with download attribute
    for a in soup.find_all("a", download=True):
        href = a.get("href", "")
        if ".pdf" in href.lower():
            filename = unquote(href.split("/")[-1])
            full_url = urljoin("https://rtec.md", href)
            pdf_response = requests.get(full_url)

            if pdf_response.status_code == 200:
                with open(f"{download_path}/{filename}", "wb") as f:
                    f.write(pdf_response.content)
                print(f"  Downloaded: {filename}")

    time.sleep(0.5)

print("Done!")
