import logging
import os
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, Depends, Form, HTTPException, File, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from itsdangerous import URLSafeTimedSerializer

from .storage import get_storage, ObjectItem, PlaceItem, LogItem, TagItem
from .storage import AuditLogItem

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
APP_SECRET = os.getenv("APP_SECRET", "xseon-real-babi-guling-1234")
PIN_HASH = os.getenv("PIN_HASH")  # hex sha256 of PIN, set via env
IS_PROD = os.getenv("IS_PROD", "false")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=APP_SECRET)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOADS_DIR), name="uploads")


def require_auth(request: Request):
    if request.session.get("auth") is True:
        return True
    raise HTTPException(status_code=401)


@app.get("/login")
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
def login_submit(request: Request, pin: str = Form(...)):
    import hashlib
    if not PIN_HASH and IS_PROD == "false":
        logging.warning("No PIN_HASH set, allowing any pin in dev mode")
        # In dev mode without PIN configured, allow any pin (warn)
        request.session["auth"] = True
        return RedirectResponse("/", status_code=302)
    pin_hash = hashlib.sha256(pin.encode()).hexdigest()
    if pin_hash == PIN_HASH:
        logging.info("PIN matched, auth granted")
        request.session["auth"] = True
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid PIN"})


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/")
def index(request: Request):
    if not request.session.get("auth"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/objects")
def list_objects(request: Request):
    require_auth(request)
    storage = get_storage()
    items = storage.list_objects()
    current_tag = request.query_params.get("tag") or ""
    if current_tag:
        items = [it for it in items if current_tag in (it.tags or [])]
    tags = storage.list_tags()
    tags_by_id = {t.id: t.name for t in tags}
    return templates.TemplateResponse(
        "objects/list.html",
        {"request": request, "items": items, "tags": tags, "tags_by_id": tags_by_id, "current_tag": current_tag},
    )


@app.get("/objects/new")
def new_object(request: Request):
    require_auth(request)
    storage = get_storage()
    places = storage.list_places()
    tags = storage.list_tags()
    tags_by_id = {t.id: t.name for t in tags}
    return templates.TemplateResponse("objects/form.html", {"request": request, "item": None, "places": places, "tags": tags, "tags_by_id": tags_by_id})


@app.post("/objects")
async def create_object(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    place_id: Optional[str] = Form(None),
    images_photo: Optional[list[UploadFile]] = File(None),
    tags: Optional[list[str]] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    # generate unique 5-digit id
    import random
    new_id = str(random.randint(10000, 99999))
    while storage.get_object(new_id):
        new_id = str(random.randint(10000, 99999))
    photos: list[str] = await _save_uploaded_photos("objects", new_id, images_photo)
    # set put_at to now if a place is specified
    put_dt = datetime.utcnow() if (place_id or "") else None
    item = ObjectItem(
        id=new_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        images_photo=photos,
        tags=[t.strip() for t in (tags or []) if t.strip()],
        place_id=place_id or "",
        put_at=put_dt,
    )
    storage.save_object(item)
    if item.place_id:
        storage.add_log(LogItem(timestamp=put_dt or datetime.utcnow(), object_id=new_id, place_id=item.place_id, notes="auto: created object"))
    return RedirectResponse("/objects", status_code=302)


@app.get("/objects/{object_id}")
def edit_object(request: Request, object_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_object(object_id)
    places = storage.list_places()
    tags = storage.list_tags()
    tags_by_id = {t.id: t.name for t in tags}
    return templates.TemplateResponse("objects/form.html", {"request": request, "item": item, "places": places, "tags": tags, "tags_by_id": tags_by_id})


@app.post("/objects/{object_id}")
async def update_object(
    request: Request,
    object_id: str,
    name: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    place_id: Optional[str] = Form(None),
    images_photo: Optional[list[UploadFile]] = File(None),
    remove_photo: Optional[list[int]] = Form(None),
    tags: Optional[list[str]] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    prev = storage.get_object(object_id)
    if not prev:
        prev = ObjectItem(id=object_id, name=name)
    photos: list[str] = prev.images_photo or []
    new_photos = await _save_uploaded_photos("objects", object_id, images_photo)
    photos = photos + new_photos
    # remove selected existing photos
    if remove_photo:
        idxs = set(int(i) for i in remove_photo if str(i).isdigit())
        photos = [p for j, p in enumerate(photos) if j not in idxs]
    # determine put_at: update to now if place changed
    new_place = place_id or ""
    put_dt = prev.put_at
    if new_place and new_place != (prev.place_id or ""):
        put_dt = datetime.utcnow()
    prev_tags = set(prev.tags or [])
    new_tags_set = set([t.strip() for t in (tags or []) if t.strip()])
    item = ObjectItem(
        id=object_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        images_photo=photos,
        tags=list(new_tags_set),
        place_id=new_place,
        put_at=put_dt,
    )
    storage.save_object(item)
    if new_place and new_place != (prev.place_id or ""):
        storage.add_log(LogItem(timestamp=put_dt or datetime.utcnow(), object_id=object_id, place_id=new_place, notes="auto: moved object"))
    added_tags = sorted(new_tags_set - prev_tags)
    removed_tags = sorted(prev_tags - new_tags_set)
    if added_tags or removed_tags:
        note_parts = []
        if added_tags:
            note_parts.append("+" + ",".join(added_tags))
        if removed_tags:
            note_parts.append("-" + ",".join(removed_tags))
        storage.add_log(LogItem(timestamp=datetime.utcnow(), object_id=object_id, place_id=new_place or (prev.place_id or ""), notes="auto: tags " + " ".join(note_parts)))
    return RedirectResponse("/objects", status_code=302)


@app.get("/places")
def list_places(request: Request):
    require_auth(request)
    storage = get_storage()
    items = storage.list_places()
    current_tag = request.query_params.get("tag") or ""
    if current_tag:
        items = [it for it in items if current_tag in (it.tags or [])]
    # Map place_id -> list of object names stored there
    objects = storage.list_objects()
    objects_by_place = {}
    for o in objects:
        if not o.place_id:
            continue
        objects_by_place.setdefault(o.place_id, []).append(o.name)
    tags = storage.list_tags()
    tags_by_id = {t.id: t.name for t in tags}
    return templates.TemplateResponse(
        "places/list.html",
        {"request": request, "items": items, "objects_by_place": objects_by_place, "tags": tags, "tags_by_id": tags_by_id, "current_tag": current_tag},
    )


@app.get("/places/new")
def new_place(request: Request):
    require_auth(request)
    storage = get_storage()
    tags = storage.list_tags()
    tags_by_id = {t.id: t.name for t in tags}
    return templates.TemplateResponse("places/form.html", {"request": request, "item": None, "tags": tags, "tags_by_id": tags_by_id})


@app.post("/places")
async def create_place(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    images_photo: Optional[list[UploadFile]] = File(None),
    tags: Optional[list[str]] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    # generate unique 5-digit id
    import random
    new_id = str(random.randint(10000, 99999))
    while storage.get_place(new_id):
        new_id = str(random.randint(10000, 99999))
    photos: list[str] = await _save_uploaded_photos("places", new_id, images_photo)
    item = PlaceItem(
        id=new_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        images_photo=photos,
        tags=[t.strip() for t in (tags or []) if t.strip()],
        put_at=None,
    )
    storage.save_place(item)
    storage.add_audit(AuditLogItem(timestamp=datetime.utcnow(), entity_type="place", entity_id=new_id, action="created", details=name))
    return RedirectResponse("/places", status_code=302)


@app.get("/places/{place_id}")
def edit_place(request: Request, place_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_place(place_id)
    objects_here = [o for o in storage.list_objects() if o.place_id == place_id]
    tags = storage.list_tags()
    tags_by_id = {t.id: t.name for t in tags}
    return templates.TemplateResponse(
        "places/form.html",
        {"request": request, "item": item, "objects_here": objects_here, "tags": tags, "tags_by_id": tags_by_id},
    )

@app.post("/places/{place_id}/delete")
def delete_place(request: Request, place_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_place(place_id)
    if not item:
        raise HTTPException(status_code=404)
    objects_here = [o for o in storage.list_objects() if o.place_id == place_id]
    if objects_here:
        tags = storage.list_tags()
        tags_by_id = {t.id: t.name for t in tags}
        return templates.TemplateResponse("places/form.html", {"request": request, "item": item, "objects_here": objects_here, "tags": tags, "tags_by_id": tags_by_id, "error": "Place has objects"})
    storage.delete_place(place_id)
    storage.add_audit(AuditLogItem(timestamp=datetime.utcnow(), entity_type="place", entity_id=place_id, action="deleted", details=item.name))
    return RedirectResponse("/places", status_code=302)

@app.post("/places/{place_id}/delete_all_objects")
def delete_all_objects(request: Request, place_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_place(place_id)
    if not item:
        raise HTTPException(status_code=404)
    count = storage.delete_objects_by_place(place_id)
    storage.add_audit(AuditLogItem(timestamp=datetime.utcnow(), entity_type="place", entity_id=place_id, action="delete_all_objects", details=str(count)))
    return RedirectResponse(f"/places/{place_id}", status_code=302)


@app.post("/places/{place_id}")
async def update_place(
    request: Request,
    place_id: str,
    name: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    images_photo: Optional[list[UploadFile]] = File(None),
    remove_photo: Optional[list[int]] = Form(None),
    tags: Optional[list[str]] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    prev = storage.get_place(place_id)
    photos: list[str] = prev.images_photo if prev else []
    new_photos = await _save_uploaded_photos("places", place_id, images_photo)
    photos = photos + new_photos
    # remove selected existing photos
    if remove_photo:
        idxs = set(int(i) for i in remove_photo if str(i).isdigit())
        photos = [p for j, p in enumerate(photos) if j not in idxs]
    item = PlaceItem(
        id=place_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        images_photo=photos,
        tags=[t.strip() for t in (tags or []) if t.strip()],
        put_at=(prev.put_at if prev else None),
    )
    storage.save_place(item)
    storage.add_audit(AuditLogItem(timestamp=datetime.utcnow(), entity_type="place", entity_id=place_id, action="updated", details=name))
    return RedirectResponse("/places", status_code=302)


@app.get("/logs")
def list_logs(request: Request):
    require_auth(request)
    storage = get_storage()
    items = storage.list_logs()
    return templates.TemplateResponse("logs/list.html", {"request": request, "items": items})


@app.get("/logs/new")
def new_log(request: Request):
    require_auth(request)
    storage = get_storage()
    objects = storage.list_objects()
    places = storage.list_places()
    return templates.TemplateResponse("logs/form.html", {"request": request, "objects": objects, "places": places})


@app.post("/logs")
def create_log(
    request: Request,
    object_id: str = Form(...),
    place_id: str = Form(...),
    notes: str = Form(""),
    at: Optional[str] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    at_dt = datetime.fromisoformat(at) if at else datetime.utcnow()
    item = LogItem(timestamp=at_dt, object_id=object_id, place_id=place_id, notes=notes)
    storage.add_log(item)
    # Also update object place
    obj = storage.get_object(object_id)
    if obj:
        obj.place_id = place_id
        obj.put_at = at_dt
        storage.save_object(obj)
    return RedirectResponse("/logs", status_code=302)


@app.get("/health")
def health():
    return {"status": "ok"}
@app.get("/audit")
def audit_list(request: Request):
    require_auth(request)
    storage = get_storage()
    items = storage.list_audit()
    return templates.TemplateResponse("audit/list.html", {"request": request, "items": items})
@app.get("/tags")
def list_tags(request: Request):
    require_auth(request)
    storage = get_storage()
    items = storage.list_tags()
    return templates.TemplateResponse("tags/list.html", {"request": request, "items": items})

@app.get("/tags/new")
def new_tag(request: Request):
    require_auth(request)
    return templates.TemplateResponse("tags/form.html", {"request": request, "item": None})

@app.post("/tags")
def create_tag(request: Request, name: str = Form(...)):
    require_auth(request)
    storage = get_storage()
    import random
    new_id = str(random.randint(10000, 99999))
    while storage.get_tag(new_id):
        new_id = str(random.randint(10000, 99999))
    storage.save_tag(TagItem(id=new_id, name=name))
    storage.add_audit(AuditLogItem(timestamp=datetime.utcnow(), entity_type="tag", entity_id=new_id, action="created", details=name))
    return RedirectResponse("/tags", status_code=302)

@app.get("/tags/{tag_id}")
def edit_tag(request: Request, tag_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_tag(tag_id)
    if not item:
        raise HTTPException(status_code=404, detail="Tag not found")
    return templates.TemplateResponse("tags/form.html", {"request": request, "item": item})

@app.post("/tags/{tag_id}")
def update_tag(request: Request, tag_id: str, name: str = Form(...)):
    require_auth(request)
    storage = get_storage()
    existing = storage.get_tag(tag_id)
    if not existing:
        existing = TagItem(id=tag_id, name=name)
    else:
        existing.name = name
    storage.save_tag(existing)
    storage.add_audit(AuditLogItem(timestamp=datetime.utcnow(), entity_type="tag", entity_id=tag_id, action="updated", details=name))
    return RedirectResponse("/tags", status_code=302)

@app.post("/tags/{tag_id}/delete")
def delete_tag(request: Request, tag_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_tag(tag_id)
    if not item:
        raise HTTPException(status_code=404)
    objs = [o for o in storage.list_objects() if tag_id in (o.tags or [])]
    places = [p for p in storage.list_places() if tag_id in (p.tags or [])]
    if objs or places:
        return templates.TemplateResponse("tags/form.html", {"request": request, "item": item, "error": "Tag is in use"})
    storage.delete_tag(tag_id)
    storage.add_audit(AuditLogItem(timestamp=datetime.utcnow(), entity_type="tag", entity_id=tag_id, action="deleted", details=item.name))
    return RedirectResponse("/tags", status_code=302)
async def _save_uploaded_photos(entity: str, entity_id: str, uploads: Optional[list[UploadFile]]) -> list[str]:
    urls: list[str] = []
    if not uploads:
        return urls
    storage = get_storage()
    for up in uploads:
        try:
            data = await up.read()
            if not data:
                continue
            mime = up.content_type or "application/octet-stream"
            fname = up.filename or "upload.bin"
            url = storage.upload_file_bytes(entity, entity_id, fname, mime, data)
            urls.append(url)
        except Exception:
            pass
    return urls
