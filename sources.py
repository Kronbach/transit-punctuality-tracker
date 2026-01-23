import json

import requests
import os



class TranzyAPI:
    def __init__(self, key, agency_id=4):
        self.url = "https://api.tranzy.ai/v1/opendata"
        self.key = key
        # Id for RTECT&PUA Chisinau agency
        self.agency_id = agency_id

    def _get(self, endpoint, with_agency=False):
        headers = {"X-API-KEY": self.key, "Accept": "application/json"}
        if with_agency:
            headers["X-Agency-Id"] = str(self.agency_id)

        response = requests.get(url= f"{self.url}/{endpoint}", headers=headers)
        response.raise_for_status()
        return response.json()

    def get_agencies(self):
        return self._get("agency")

    def get_vehicles(self):
        return self._get("vehicles", True)

    def get_routes(self):
        return self._get("routes", True)

    def get_trips(self):
        return self._get("trips", True)

    def get_stop_times(self):
        return self._get("stop_times", True)

    def get_stops(self):
        return self._get("stops", True)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    api = TranzyAPI(os.getenv("TRANZY_KEY"))

    vehicle_data = api.get_vehicles()
    i = 0
    rows = []
    for item in vehicle_data:
        if i < 10:
            i += 1
            rows.append([json.dumps(item)])


    print(rows)
