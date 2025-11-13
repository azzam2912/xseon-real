import os
import csv
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict


@dataclass
class ObjectItem:
    id: str
    name: str
    description: str = ""
    images: List[str] = field(default_factory=list)
    images_photo: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    place_id: str = ""
    put_at: Optional[datetime] = None


@dataclass
class PlaceItem:
    id: str
    name: str
    description: str = ""
    images: List[str] = field(default_factory=list)
    images_photo: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    put_at: Optional[datetime] = None


@dataclass
class LogItem:
    timestamp: datetime
    object_id: str
    place_id: str
    notes: str = ""


@dataclass
class TagItem:
    id: str
    name: str


@dataclass
class AuditLogItem:
    timestamp: datetime
    entity_type: str
    entity_id: str
    action: str
    details: str = ""


DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class LocalCsvBackend:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.objects_path = os.path.join(DATA_DIR, "objects.csv")
        self.places_path = os.path.join(DATA_DIR, "places.csv")
        self.logs_path = os.path.join(DATA_DIR, "logs.csv")
        self.tags_path = os.path.join(DATA_DIR, "tags.csv")
        self.audit_path = os.path.join(DATA_DIR, "audit.csv")
        # Ensure headers
        self._ensure_file(self.objects_path, ["id", "name", "description", "images", "images_photo", "tags", "place_id", "put_at"])
        self._ensure_file(self.places_path, ["id", "name", "description", "images", "images_photo", "tags", "put_at"])
        self._ensure_file(self.logs_path, ["timestamp", "object_id", "place_id", "notes"])
        self._ensure_file(self.tags_path, ["id", "name"])
        self._ensure_file(self.audit_path, ["timestamp", "entity_type", "entity_id", "action", "details"])
        # local uploads directory for non-Google setups
        self.uploads_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
        os.makedirs(self.uploads_root, exist_ok=True)

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
                    images_photo=[i for i in (row.get("images_photo") or "").split("|") if i],
                    tags=[t for t in (row.get("tags") or "").split("|") if t],
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
                    "images_photo": "|".join(item.images_photo or []),
                    "tags": "|".join(item.tags or []),
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
                "images_photo": "|".join(item.images_photo or []),
                "tags": "|".join(item.tags or []),
                "place_id": item.place_id,
                "put_at": item.put_at.isoformat() if item.put_at else "",
            })
        with open(self.objects_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "description", "images", "images_photo", "tags", "place_id", "put_at"])
            writer.writeheader()
            writer.writerows(rows)

    def delete_object(self, object_id: str):
        rows: List[Dict[str, str]] = []
        with open(self.objects_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader if r.get("id") != object_id]
        with open(self.objects_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "description", "images", "images_photo", "tags", "place_id", "put_at"])
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
                    images_photo=[i for i in (row.get("images_photo") or "").split("|") if i],
                    tags=[t for t in (row.get("tags") or "").split("|") if t],
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
                    "images_photo": "|".join(item.images_photo or []),
                    "tags": "|".join(item.tags or []),
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
                "images_photo": "|".join(item.images_photo or []),
                "tags": "|".join(item.tags or []),
                "put_at": item.put_at.isoformat() if item.put_at else "",
            })
        with open(self.places_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "description", "images", "images_photo", "tags", "put_at"])
            writer.writeheader()
            writer.writerows(rows)

    def delete_place(self, place_id: str):
        rows: List[Dict[str, str]] = []
        with open(self.places_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader if r.get("id") != place_id]
        with open(self.places_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "description", "images", "images_photo", "tags", "put_at"])
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

    def upload_file_bytes(self, entity: str, entity_id: str, filename: str, mime: str, data: bytes) -> str:
        import time
        import random
        # Save to local uploads and return URL path
        subdir = os.path.join(self.uploads_root, entity, entity_id)
        os.makedirs(subdir, exist_ok=True)
        ext = os.path.splitext(filename)[1] or ".bin"
        fname = f"photo_{int(time.time())}_{random.randint(1000,9999)}{ext}"
        fpath = os.path.join(subdir, fname)
        with open(fpath, "wb") as fh:
            fh.write(data)
        return f"/uploads/{entity}/{entity_id}/{fname}"

    def list_audit(self) -> List[AuditLogItem]:
        items: List[AuditLogItem] = []
        with open(self.audit_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                items.append(AuditLogItem(
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    entity_type=row.get("entity_type", ""),
                    entity_id=row.get("entity_id", ""),
                    action=row.get("action", ""),
                    details=row.get("details", ""),
                ))
        return items

    def add_audit(self, item: AuditLogItem):
        rows: List[Dict[str, str]] = []
        with open(self.audit_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        rows.append({
            "timestamp": item.timestamp.isoformat(),
            "entity_type": item.entity_type,
            "entity_id": item.entity_id,
            "action": item.action,
            "details": item.details,
        })
        with open(self.audit_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "entity_type", "entity_id", "action", "details"])
            writer.writeheader()
            writer.writerows(rows)

    def list_tags(self) -> List[TagItem]:
        items: List[TagItem] = []
        with open(self.tags_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                items.append(TagItem(id=row["id"], name=row["name"]))
        return items

    def get_tag(self, tag_id: str) -> Optional[TagItem]:
        for t in self.list_tags():
            if t.id == tag_id:
                return t
        return None

    def save_tag(self, item: TagItem):
        rows: List[Dict[str, str]] = []
        found = False
        with open(self.tags_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        for r in rows:
            if r["id"] == item.id:
                r.update({"id": item.id, "name": item.name})
                found = True
                break
        if not found:
            rows.append({"id": item.id, "name": item.name})
        with open(self.tags_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"])
            writer.writeheader()
            writer.writerows(rows)

    def delete_tag(self, tag_id: str):
        rows: List[Dict[str, str]] = []
        with open(self.tags_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader if r.get("id") != tag_id]
        with open(self.tags_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"])
            writer.writeheader()
            writer.writerows(rows)

    def delete_objects_by_place(self, place_id: str) -> int:
        orig_rows: List[Dict[str, str]] = []
        with open(self.objects_path, newline="") as f:
            reader = csv.DictReader(f)
            orig_rows = list(reader)
        remaining = [r for r in orig_rows if r.get("place_id") != place_id]
        deleted_count = len(orig_rows) - len(remaining)
        with open(self.objects_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "description", "images", "images_photo", "tags", "place_id", "put_at"])
            writer.writeheader()
            writer.writerows(remaining)
        return deleted_count


class GoogleSheetsBackend:
    def __init__(self):
        import gspread
        from google.oauth2.service_account import Credentials

        spreadsheet_name = os.getenv("SPREADSHEET_NAME")
        spreadsheet_id = os.getenv("SPREADSHEET_ID")
        sa_json_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        sa_json_text = os.getenv("GOOGLE_SERVICE_ACCOUNT_INFO")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ]
        if sa_json_text:
            import json
            info = json.loads(sa_json_text)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
        elif sa_json_path:
            creds = Credentials.from_service_account_file(sa_json_path, scopes=scopes)
        else:
            raise RuntimeError("Google service account credentials not configured.")

        gc = gspread.authorize(creds)
        if spreadsheet_id:
            self.spreadsheet = gc.open_by_key(spreadsheet_id)
        elif spreadsheet_name:
            self.spreadsheet = gc.open(spreadsheet_name)
        else:
            raise RuntimeError("SPREADSHEET_ID or SPREADSHEET_NAME env var not set.")
        self.creds = creds
        from googleapiclient.discovery import build
        self.drive_service = build("drive", "v3", credentials=self.creds)
        self.upload_folder_id = os.getenv("GOOGLE_DRIVE_UPLOAD_FOLDER_ID") or self._ensure_upload_folder()
        self.objects_ws = self._get_or_create_ws("Objects", ["id", "name", "description", "images", "images_photo", "tags", "place_id", "put_at"])
        self.places_ws = self._get_or_create_ws("Places", ["id", "name", "description", "images", "images_photo", "tags", "put_at"])
        self.logs_ws = self._get_or_create_ws("Logs", ["timestamp", "object_id", "place_id", "notes"])
        self.tags_ws = self._get_or_create_ws("Tags", ["id", "name"])
        self.audit_ws = self._get_or_create_ws("Audit", ["timestamp", "entity_type", "entity_id", "action", "details"])

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

    def _ensure_upload_folder(self) -> str:
        # Try to find existing folder named XseonUploads; create if not exists
        try:
            q = "name = 'XseonUploads' and mimeType = 'application/vnd.google-apps.folder'"
            res = self.drive_service.files().list(q=q, fields="files(id,name)", pageSize=1).execute()
            files = res.get("files", [])
            if files:
                return files[0]["id"]
            metadata = {"name": "XseonUploads", "mimeType": "application/vnd.google-apps.folder"}
            folder = self.drive_service.files().create(body=metadata, fields="id").execute()
            # Make public readable
            try:
                self.drive_service.permissions().create(fileId=folder["id"], body={"type": "anyone", "role": "reader"}).execute()
            except Exception:
                pass
            return folder["id"]
        except Exception:
            # Fallback: create folder
            metadata = {"name": "XseonUploads", "mimeType": "application/vnd.google-apps.folder"}
            folder = self.drive_service.files().create(body=metadata, fields="id").execute()
            return folder["id"]

    def list_objects(self) -> List[ObjectItem]:
        rows = self.objects_ws.get_all_records()
        items = []
        for r in rows:
            items.append(ObjectItem(
                id=str(r.get("id", "")),
                name=str(r.get("name", "")),
                description=str(r.get("description", "")),
                images=[i for i in str(r.get("images", "")).split("|") if i],
                images_photo=[i for i in str(r.get("images_photo", "")).split("|") if i],
                tags=[t for t in str(r.get("tags", "")).split("|") if t],
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
        headers = ["id", "name", "description", "images", "images_photo", "tags", "place_id", "put_at"]
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
            "|".join(item.images_photo or []),
            "|".join(item.tags or []),
            item.place_id,
            item.put_at.isoformat() if item.put_at else "",
        ]
        if found_row_index:
            self.objects_ws.update(f"A{found_row_index}", [row_values])
        else:
            self.objects_ws.append_row(row_values)

    def delete_object(self, object_id: str):
        rows = self.objects_ws.get_all_records()
        to_delete = None
        for idx, r in enumerate(rows, start=2):
            if str(r.get("id")) == object_id:
                to_delete = idx
                break
        if to_delete:
            self.objects_ws.delete_rows(to_delete)

    def list_places(self) -> List[PlaceItem]:
        rows = self.places_ws.get_all_records()
        items = []
        for r in rows:
            items.append(PlaceItem(
                id=str(r.get("id", "")),
                name=str(r.get("name", "")),
                description=str(r.get("description", "")),
                images=[i for i in str(r.get("images", "")).split("|") if i],
                images_photo=[i for i in str(r.get("images_photo", "")).split("|") if i],
                tags=[t for t in str(r.get("tags", "")).split("|") if t],
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
            "|".join(item.images_photo or []),
            "|".join(item.tags or []),
            item.put_at.isoformat() if item.put_at else "",
        ]
        if found_row_index:
            self.places_ws.update(f"A{found_row_index}", [row_values])
        else:
            self.places_ws.append_row(row_values)

    def delete_place(self, place_id: str):
        rows = self.places_ws.get_all_records()
        to_delete = None
        for idx, r in enumerate(rows, start=2):
            if str(r.get("id")) == place_id:
                to_delete = idx
                break
        if to_delete:
            self.places_ws.delete_rows(to_delete)

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

    def list_audit(self) -> List[AuditLogItem]:
        rows = self.audit_ws.get_all_records()
        items: List[AuditLogItem] = []
        for r in rows:
            items.append(AuditLogItem(
                timestamp=datetime.fromisoformat(str(r.get("timestamp"))),
                entity_type=str(r.get("entity_type", "")),
                entity_id=str(r.get("entity_id", "")),
                action=str(r.get("action", "")),
                details=str(r.get("details", "")),
            ))
        return items

    def list_tags(self) -> List[TagItem]:
        rows = self.tags_ws.get_all_records()
        return [TagItem(id=str(r.get("id", "")), name=str(r.get("name", ""))) for r in rows]

    def get_tag(self, tag_id: str) -> Optional[TagItem]:
        for t in self.list_tags():
            if t.id == tag_id:
                return t
        return None

    def save_tag(self, item: TagItem):
        rows = self.tags_ws.get_all_records()
        found_row_index = None
        for idx, r in enumerate(rows, start=2):
            if str(r.get("id")) == item.id:
                found_row_index = idx
                break
        row_values = [item.id, item.name]
        if found_row_index:
            self.tags_ws.update(f"A{found_row_index}", [row_values])
        else:
            self.tags_ws.append_row(row_values)

    def delete_tag(self, tag_id: str):
        rows = self.tags_ws.get_all_records()
        to_delete = None
        for idx, r in enumerate(rows, start=2):
            if str(r.get("id")) == tag_id:
                to_delete = idx
                break
        if to_delete:
            self.tags_ws.delete_rows(to_delete)

    def add_log(self, item: LogItem):
        row_values = [
            item.timestamp.isoformat(),
            item.object_id,
            item.place_id,
            item.notes,
        ]
        self.logs_ws.append_row(row_values)

    def upload_file_bytes(self, entity: str, entity_id: str, filename: str, mime: str, data: bytes) -> str:
        import io
        from googleapiclient.http import MediaIoBaseUpload
        name = f"{entity}_{entity_id}_{filename}"
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
        body = {"name": name, "parents": [self.upload_folder_id]}
        file = self.drive_service.files().create(body=body, media_body=media, fields="id").execute()
        fid = file.get("id")
        try:
            self.drive_service.permissions().create(fileId=fid, body={"type": "anyone", "role": "reader"}).execute()
        except Exception:
            pass
        return f"https://drive.google.com/uc?id={fid}"

    def add_audit(self, item: AuditLogItem):
        row_values = [
            item.timestamp.isoformat(),
            item.entity_type,
            item.entity_id,
            item.action,
            item.details,
        ]
        self.audit_ws.append_row(row_values)

    def delete_objects_by_place(self, place_id: str) -> int:
        rows = self.objects_ws.get_all_records()
        idxs = []
        for idx, r in enumerate(rows, start=2):
            if str(r.get("place_id", "")) == place_id:
                idxs.append(idx)
        for i in sorted(idxs, reverse=True):
            self.objects_ws.delete_rows(i)
        return len(idxs)


def get_storage():
    use_google = os.getenv("USE_GOOGLE_SHEETS", "false").lower() == "true"
    if use_google:
        return GoogleSheetsBackend()
    return LocalCsvBackend()
