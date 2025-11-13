import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from itsdangerous import URLSafeTimedSerializer

from .storage import get_storage, ObjectItem, PlaceItem, LogItem


APP_SECRET = os.getenv("APP_SECRET", "dev-secret-change-me")
PIN_HASH = os.getenv("PIN_HASH")  # hex sha256 of PIN, set via env

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=APP_SECRET)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


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
    if not PIN_HASH:
        # In dev mode without PIN configured, allow any pin (warn)
        request.session["auth"] = True
        return RedirectResponse("/", status_code=302)
    pin_hash = hashlib.sha256(pin.encode()).hexdigest()
    if pin_hash == PIN_HASH:
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
    return templates.TemplateResponse("objects/list.html", {"request": request, "items": items})


@app.get("/objects/new")
def new_object(request: Request):
    require_auth(request)
    storage = get_storage()
    places = storage.list_places()
    return templates.TemplateResponse("objects/form.html", {"request": request, "item": None, "places": places})


@app.post("/objects")
def create_object(
    request: Request,
    name: str = Form(...),
    object_id: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    place_id: Optional[str] = Form(None),
    put_at: Optional[str] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    put_dt = None
    if put_at:
        try:
            put_dt = datetime.fromisoformat(put_at)
        except ValueError:
            put_dt = None
    item = ObjectItem(
        id=object_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        place_id=place_id or "",
        put_at=put_dt,
    )
    storage.save_object(item)
    return RedirectResponse("/objects", status_code=302)


@app.get("/objects/{object_id}")
def edit_object(request: Request, object_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_object(object_id)
    places = storage.list_places()
    return templates.TemplateResponse("objects/form.html", {"request": request, "item": item, "places": places})


@app.post("/objects/{object_id}")
def update_object(
    request: Request,
    object_id: str,
    name: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    place_id: Optional[str] = Form(None),
    put_at: Optional[str] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    put_dt = None
    if put_at:
        try:
            put_dt = datetime.fromisoformat(put_at)
        except ValueError:
            put_dt = None
    item = ObjectItem(
        id=object_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        place_id=place_id or "",
        put_at=put_dt,
    )
    storage.save_object(item)
    return RedirectResponse("/objects", status_code=302)


@app.get("/places")
def list_places(request: Request):
    require_auth(request)
    storage = get_storage()
    items = storage.list_places()
    return templates.TemplateResponse("places/list.html", {"request": request, "items": items})


@app.get("/places/new")
def new_place(request: Request):
    require_auth(request)
    return templates.TemplateResponse("places/form.html", {"request": request, "item": None})


@app.post("/places")
def create_place(
    request: Request,
    name: str = Form(...),
    place_id: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    put_at: Optional[str] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    put_dt = None
    if put_at:
        try:
            put_dt = datetime.fromisoformat(put_at)
        except ValueError:
            put_dt = None
    item = PlaceItem(
        id=place_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        put_at=put_dt,
    )
    storage.save_place(item)
    return RedirectResponse("/places", status_code=302)


@app.get("/places/{place_id}")
def edit_place(request: Request, place_id: str):
    require_auth(request)
    storage = get_storage()
    item = storage.get_place(place_id)
    return templates.TemplateResponse("places/form.html", {"request": request, "item": item})


@app.post("/places/{place_id}")
def update_place(
    request: Request,
    place_id: str,
    name: str = Form(...),
    description: str = Form(""),
    images: str = Form(""),
    put_at: Optional[str] = Form(None),
):
    require_auth(request)
    storage = get_storage()
    put_dt = None
    if put_at:
        try:
            put_dt = datetime.fromisoformat(put_at)
        except ValueError:
            put_dt = None
    item = PlaceItem(
        id=place_id,
        name=name,
        description=description,
        images=[i.strip() for i in images.split(",") if i.strip()],
        put_at=put_dt,
    )
    storage.save_place(item)
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