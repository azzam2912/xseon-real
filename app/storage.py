import os
import csv
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict


@dataclass
class ObjectItem:
    id: str
    name: str
    description: str = ""
    images: List[str] = None
    place_id: str = ""
    put_at: Optional[datetime] = None


@dataclass
class PlaceItem:
    id: str
    name: str
    description: str = ""
    images: List[str] = None
    put_at: Optional[datetime] = None


@dataclass
class LogItem:
    timestamp: datetime
    object_id: str
    place_id: str
    notes: str = ""


DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class LocalCsvBackend:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.objects_path = os.path.join(DATA_DIR, "objects.csv")
        self.places_path = os.path.join(DATA_DIR, "places.csv")
        self.logs_path = os.path.join(DATA_DIR, "logs.csv")
        # Ensure headers
        self._ensure_file(self.objects_path, ["id", "name", "description", "images", "place_id", "put_at"])
        self._ensure_file(self.places_path, ["id", "name", "description", "images", "put_at"])
        self._ensure_file(self.logs_path, ["timestamp", "object_id", "place_id", "notes"])

    def _ensure_file(self, path: str, headers: List[str]):
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

    def list_objects(self) -> List[ObjectItem]:
        items = []
        with open(self.objects_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                items.append(ObjectItem(
                    id=row["id"],
                    name=row["name"],
                    description=row.get("description", ""),
                    images=[i for i in (row.get("images") or "").split("|") if i],
                    place_id=row.get("place_id", ""),
                    put_at=datetime.fromisoformat(row["put_at"]) if row.get("put_at") else None,
                ))
        return items

    def get_object(self, object_id: str) -> Optional[ObjectItem]:
        for item in self.list_objects():
            if item.id == object_id:
                return item
        return None

    def save_object(self, item: ObjectItem):
        rows: List[Dict[str, str]] = []
        found = False
        with open(self.objects_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        for r in rows:
            if r["id"] == item.id:
                r.update({
                    "id": item.id,
                    "name": item.name,
                    "description": item.description,
                    "images": "|".join(item.images or []),
                    "place_id": item.place_id,
                    "put_at": item.put_at.isoformat() if item.put_at else "",
                })
                found = True
                break
        if not found:
            rows.append({
                "id": item.id,
                "name": item.name,
                "description": item.description,
                "images": "|".join(item.images or []),
                "place_id": item.place_id,
                "put_at": item.put_at.isoformat() if item.put_at else "",
            })
        with open(self.objects_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "description", "images", "place_id", "put_at"])
            writer.writeheader()
            writer.writerows(rows)

    def list_places(self) -> List[PlaceItem]:
        items = []
        with open(self.places_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                items.append(PlaceItem(
                    id=row["id"],
                    name=row["name"],
                    description=row.get("description", ""),
                    images=[i for i in (row.get("images") or "").split("|") if i],
                    put_at=datetime.fromisoformat(row["put_at"]) if row.get("put_at") else None,
                ))
        return items

    def get_place(self, place_id: str) -> Optional[PlaceItem]:
        for item in self.list_places():
            if item.id == place_id:
                return item
        return None

    def save_place(self, item: PlaceItem):
        rows: List[Dict[str, str]] = []
        found = False
        with open(self.places_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        for r in rows:
            if r["id"] == item.id:
                r.update({
                    "id": item.id,
                    "name": item.name,
                    "description": item.description,
                    "images": "|".join(item.images or []),
                    "put_at": item.put_at.isoformat() if item.put_at else "",
                })
                found = True
                break
        if not found:
            rows.append({
                "id": item.id,
                "name": item.name,
                "description": item.description,
                "images": "|".join(item.images or []),
                "put_at": item.put_at.isoformat() if item.put_at else "",
            })
        with open(self.places_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "description", "images", "put_at"])
            writer.writeheader()
            writer.writerows(rows)

    def list_logs(self) -> List[LogItem]:
        items = []
        with open(self.logs_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                items.append(LogItem(
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    object_id=row["object_id"],
                    place_id=row["place_id"],
                    notes=row.get("notes", ""),
                ))
        return items

    def add_log(self, item: LogItem):
        rows: List[Dict[str, str]] = []
        with open(self.logs_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        rows.append({
            "timestamp": item.timestamp.isoformat(),
            "object_id": item.object_id,
            "place_id": item.place_id,
            "notes": item.notes,
        })
        with open(self.logs_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "object_id", "place_id", "notes"])
            writer.writeheader()
            writer.writerows(rows)


class GoogleSheetsBackend:
    def __init__(self):
        import gspread
        from google.oauth2.service_account import Credentials

        spreadsheet_name = os.getenv("SPREADSHEET_NAME")
        sa_json_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        sa_json_text = os.getenv("GOOGLE_SERVICE_ACCOUNT_INFO")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        if sa_json_text:
            import json
            info = json.loads(sa_json_text)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
        elif sa_json_path:
            creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        else:
            raise RuntimeError("Google service account credentials not configured.")

        if not spreadsheet_name:
            raise RuntimeError("SPREADSHEET_NAME env var not set.")
        gc = gspread.authorize(creds)
        self.spreadsheet = gc.open(spreadsheet_name)
        self.objects_ws = self._get_or_create_ws("Objects", ["id", "name", "description", "images", "place_id", "put_at"])
        self.places_ws = self._get_or_create_ws("Places", ["id", "name", "description", "images", "put_at"])
        self.logs_ws = self._get_or_create_ws("Logs", ["timestamp", "object_id", "place_id", "notes"])

    def _get_or_create_ws(self, title: str, headers: List[str]):
        try:
            ws = self.spreadsheet.worksheet(title)
        except Exception:
            ws = self.spreadsheet.add_worksheet(title=title, rows=100, cols=len(headers))
            ws.append_row(headers)
        # ensure headers
        first_row = ws.row_values(1)
        if first_row != headers:
            ws.update("A1", [headers])
        return ws

    def list_objects(self) -> List[ObjectItem]:
        rows = self.objects_ws.get_all_records()
        items = []
        for r in rows:
            items.append(ObjectItem(
                id=str(r.get("id", "")),
                name=str(r.get("name", "")),
                description=str(r.get("description", "")),
                images=[i for i in str(r.get("images", "")).split("|") if i],
                place_id=str(r.get("place_id", "")),
                put_at=datetime.fromisoformat(r["put_at"]) if r.get("put_at") else None,
            ))
        return items

    def get_object(self, object_id: str) -> Optional[ObjectItem]:
        for item in self.list_objects():
            if item.id == object_id:
                return item
        return None

    def save_object(self, item: ObjectItem):
        rows = self.objects_ws.get_all_records()
        headers = ["id", "name", "description", "images", "place_id", "put_at"]
        found_row_index = None
        for idx, r in enumerate(rows, start=2):
            if str(r.get("id")) == item.id:
                found_row_index = idx
                break
        row_values = [
            item.id,
            item.name,
            item.description,
            "|".join(item.images or []),
            item.place_id,
            item.put_at.isoformat() if item.put_at else "",
        ]
        if found_row_index:
            self.objects_ws.update(f"A{found_row_index}", [row_values])
        else:
            self.objects_ws.append_row(row_values)

    def list_places(self) -> List[PlaceItem]:
        rows = self.places_ws.get_all_records()
        items = []
        for r in rows:
            items.append(PlaceItem(
                id=str(r.get("id", "")),
                name=str(r.get("name", "")),
                description=str(r.get("description", "")),
                images=[i for i in str(r.get("images", "")).split("|") if i],
                put_at=datetime.fromisoformat(r["put_at"]) if r.get("put_at") else None,
            ))
        return items

    def get_place(self, place_id: str) -> Optional[PlaceItem]:
        for item in self.list_places():
            if item.id == place_id:
                return item
        return None

    def save_place(self, item: PlaceItem):
        rows = self.places_ws.get_all_records()
        found_row_index = None
        for idx, r in enumerate(rows, start=2):
            if str(r.get("id")) == item.id:
                found_row_index = idx
                break
        row_values = [
            item.id,
            item.name,
            item.description,
            "|".join(item.images or []),
            item.put_at.isoformat() if item.put_at else "",
        ]
        if found_row_index:
            self.places_ws.update(f"A{found_row_index}", [row_values])
        else:
            self.places_ws.append_row(row_values)

    def list_logs(self) -> List[LogItem]:
        rows = self.logs_ws.get_all_records()
        items = []
        for r in rows:
            items.append(LogItem(
                timestamp=datetime.fromisoformat(r["timestamp"]),
                object_id=str(r.get("object_id", "")),
                place_id=str(r.get("place_id", "")),
                notes=str(r.get("notes", "")),
            ))
        return items

    def add_log(self, item: LogItem):
        row_values = [
            item.timestamp.isoformat(),
            item.object_id,
            item.place_id,
            item.notes,
        ]
        self.logs_ws.append_row(row_values)


def get_storage():
    use_google = os.getenv("USE_GOOGLE_SHEETS", "false").lower() == "true"
    if use_google:
        return GoogleSheetsBackend()
    return LocalCsvBackend()