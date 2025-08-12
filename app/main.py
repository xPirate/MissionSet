
import os
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy import create_engine, text
from passlib.hash import bcrypt

from opensearchpy import OpenSearch

# ---- Config ----
OS_HOST = os.getenv("OS_HOST", "localhost")
OS_PORT = int(os.getenv("OS_PORT", "9200"))
OS_INDEX = os.getenv("OS_INDEX", "missionset-data")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data.db")
SECRET_KEY = os.getenv("SECRET_KEY", "CHANGE_ME_SECRET")

ALLOWED_LABELS = ["Recon", "Mission", "Medical", "Emergency", "Notice"]

# ---- App ----
app = FastAPI(title="MissionSet Auth")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, session_cookie="ms_session")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---- DB ----
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)

def init_db():
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                tags TEXT,
                created_at TEXT NOT NULL,
                author TEXT,
                author_user_id INTEGER,
                start_time TEXT,
                end_time TEXT
            );
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS profiles (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                birthday TEXT,
                blood_type TEXT,
                team TEXT,
                team_role TEXT,
                phone TEXT,
                email TEXT,
                contact_name TEXT,
                contact_phone TEXT,
                updated_at TEXT
            );
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
init_db()

# ---- OpenSearch ----
os_client = OpenSearch(
    hosts=[{"host": OS_HOST, "port": OS_PORT}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
)

def ensure_index():
    try:
        if not os_client.indices.exists(index=OS_INDEX):
            os_client.indices.create(
                index=OS_INDEX,
                body={
                    "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
                    "mappings": {
                        "properties": {
                            "title": {"type": "text"},
                            "description": {"type": "text"},
                            "tags": {"type": "keyword"},
                            "author": {"type": "keyword"},
                            "start_time": {"type": "date"},
                            "end_time": {"type": "date"},
                            "created_at": {"type": "date"},
                        }
                    },
                },
            )
    except Exception as e:
        print("OpenSearch index init error:", e)

ensure_index()

# ---- Helpers ----
def row_to_dict(row): return dict(row._mapping) if row else None

def current_user(request: Request) -> Optional[Dict]:
    uid = request.session.get("user_id")
    if not uid: return None
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM users WHERE id = :id AND is_active=1"), {"id": uid}).first()
    return row_to_dict(row) if row else None

def get_profile(uid: int) -> Dict:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM profiles WHERE user_id = :id"), {"id": uid}).first()
    return row_to_dict(row) or {"user_id": uid, "name": "", "birthday": "", "blood_type": "", "team": "", "team_role": "", "phone": "", "email": "", "contact_name": "", "contact_phone": ""}

def ctx(request: Request, extra: Dict=None) -> Dict:
    u = current_user(request)
    p = get_profile(u["id"]) if u else None
    base = {"request": request, "user": u, "profile": p, "ALLOWED_LABELS": ALLOWED_LABELS}
    if extra: base.update(extra)
    return base

def get_last_n_days(n: int):
    today = datetime.utcnow().date()
    return [(today - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]

def parse_dt(s: str, field: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid datetime for {field}")

def index_item_in_os(item_id: int, item: Dict):
    try:
        os_client.index(
            index=OS_INDEX,
            id=str(item_id),
            body={
                "title": item["title"],
                "description": item.get("description") or "",
                "tags": [t.strip() for t in (item.get("tags") or "").split(",") if t.strip()],
                "author": item.get("author") or "",
                "start_time": item.get("start_time"),
                "end_time": item.get("end_time"),
                "created_at": item["created_at"],
            },
            refresh=True,
        )
    except Exception as e:
        print("OpenSearch index error:", e)

# ---- Auth ----
@app.get("/auth/login", response_class=HTMLResponse)
def login_form(request: Request):
    return templates.TemplateResponse("login.html", ctx(request, {"error": None}))

@app.post("/auth/login")
def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM users WHERE username = :u AND is_active=1"), {"u": username}).first()
    user = row_to_dict(row) if row else None
    if not user or not bcrypt.verify(password, user["password_hash"]):
        return templates.TemplateResponse("login.html", ctx(request, {"error": "Invalid credentials"}), status_code=401)
    request.session["user_id"] = user["id"]
    return RedirectResponse(url="/", status_code=303)

@app.post("/auth/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/auth/login", status_code=303)

@app.get("/auth/register", response_class=HTMLResponse)
def register_form(request: Request):
    # If first user, open self-signup; else admin-only
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) AS c FROM users")).scalar_one()
    if count > 0:
        u = current_user(request)
        if not u or not u.get("is_admin"):
            return RedirectResponse(url="/auth/login", status_code=303)
    return templates.TemplateResponse("register.html", ctx(request, {"error": None}))

@app.post("/auth/register")
def register_submit(request: Request, username: str = Form(...), password: str = Form(...), name: str = Form(...)):
    now = datetime.utcnow().isoformat()
    pwd = bcrypt.hash(password)
    with engine.begin() as conn:
        count = conn.execute(text("SELECT COUNT(*) AS c FROM users")).scalar_one()
        is_admin = 1 if count == 0 else 0
        try:
            conn.execute(text("INSERT INTO users (username, password_hash, is_admin, is_active, created_at) VALUES (:u,:p,:a,1,:c)"),
                         {"u": username, "p": pwd, "a": is_admin, "c": now})
        except Exception:
            return templates.TemplateResponse("register.html", ctx(request, {"error": "Username already exists"}), status_code=400)
        uid = conn.execute(text("SELECT id FROM users WHERE username=:u"), {"u": username}).scalar_one()
        conn.execute(text("INSERT INTO profiles (user_id, name, updated_at) VALUES (:id,:n,:t) ON CONFLICT(user_id) DO UPDATE SET name=:n, updated_at=:t"),
                     {"id": uid, "n": name, "t": now})
    return RedirectResponse(url="/auth/login", status_code=303)

# ---- Admin Users ----
@app.get("/admin/users", response_class=HTMLResponse)
def admin_users(request: Request):
    u = current_user(request)
    if not u or not u.get("is_admin"):
        return RedirectResponse(url="/auth/login", status_code=303)
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT u.*, COALESCE(p.name,'') AS name FROM users u LEFT JOIN profiles p ON p.user_id=u.id ORDER BY u.id")).all()
    users = [row_to_dict(r) for r in rows]
    return templates.TemplateResponse("admin_users.html", ctx(request, {"users": users}))

@app.get("/admin/users/new", response_class=HTMLResponse)
def admin_users_new_form(request: Request):
    u = current_user(request)
    if not u or not u.get("is_admin"):
        return RedirectResponse(url="/auth/login", status_code=303)
    return templates.TemplateResponse("admin_user_new.html", ctx(request, {"error": None}))

@app.post("/admin/users/new")
def admin_users_new(request: Request, username: str = Form(...), password: str = Form(...), name: str = Form(...), is_admin: int = Form(0)):
    u = current_user(request)
    if not u or not u.get("is_admin"):
        return RedirectResponse(url="/auth/login", status_code=303)
    now = datetime.utcnow().isoformat()
    pwd = bcrypt.hash(password)
    with engine.begin() as conn:
        try:
            conn.execute(text("INSERT INTO users (username,password_hash,is_admin,is_active,created_at) VALUES (:u,:p,:a,1,:c)"),
                         {"u": username, "p": pwd, "a": 1 if is_admin else 0, "c": now})
        except Exception:
            return templates.TemplateResponse("admin_user_new.html", ctx(request, {"error": "Username exists"}), status_code=400)
        uid = conn.execute(text("SELECT id FROM users WHERE username=:u"), {"u": username}).scalar_one()
        conn.execute(text("INSERT INTO profiles (user_id,name,updated_at) VALUES (:id,:n,:t)"), {"id": uid, "n": name, "t": now})
    return RedirectResponse(url="/admin/users", status_code=303)

@app.post("/admin/users/{uid}/toggle_admin")
def admin_toggle_admin(request: Request, uid: int):
    u = current_user(request)
    if not u or not u.get("is_admin"):
        return RedirectResponse(url="/auth/login", status_code=303)
    with engine.begin() as conn:
        conn.execute(text("UPDATE users SET is_admin = 1 - is_admin WHERE id = :id"), {"id": uid})
    return RedirectResponse(url="/admin/users", status_code=303)

@app.post("/admin/users/{uid}/toggle_active")
def admin_toggle_active(request: Request, uid: int):
    u = current_user(request)
    if not u or not u.get("is_admin"):
        return RedirectResponse(url="/auth/login", status_code=303)
    with engine.begin() as conn:
        conn.execute(text("UPDATE users SET is_active = 1 - is_active WHERE id = :id"), {"id": uid})
    return RedirectResponse(url="/admin/users", status_code=303)

# ---- Profile ----
@app.get("/profile", response_class=HTMLResponse)
def profile_form(request: Request):
    u = current_user(request)
    if not u:
        return RedirectResponse(url="/auth/login", status_code=303)
    prof = get_profile(u["id"])
    return templates.TemplateResponse("profile.html", ctx(request, {"prof": prof}))

@app.post("/profile")
def profile_submit(
    request: Request,
    name: str = Form(...),
    birthday: str = Form(""),
    blood_type: str = Form(""),
    team: str = Form(""),
    team_role: str = Form(""),
    phone: str = Form(""),
    email: str = Form(""),
    contact_name: str = Form(""),
    contact_phone: str = Form(""),
):
    u = current_user(request)
    if not u:
        return RedirectResponse(url="/auth/login", status_code=303)
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO profiles (user_id, name, birthday, blood_type, team, team_role, phone, email, contact_name, contact_phone, updated_at)
            VALUES (:uid,:name,:bd,:bt,:tm,:tr,:ph,:em,:cn,:cp,:now)
            ON CONFLICT(user_id) DO UPDATE SET
              name=:name, birthday=:bd, blood_type=:bt, team=:tm, team_role=:tr,
              phone=:ph, email=:em, contact_name=:cn, contact_phone=:cp, updated_at=:now
        """), {"uid": u["id"], "name": name, "bd": birthday, "bt": blood_type, "tm": team, "tr": team_role,
               "ph": phone, "em": email, "cn": contact_name, "cp": contact_phone, "now": now})
    return RedirectResponse(url="/profile", status_code=303)

# ---- Dashboard ----
def compute_dashboard_stats():
    with engine.connect() as conn:
        recent_rows = conn.execute(text("SELECT id,title,created_at FROM items ORDER BY id DESC LIMIT 5")).all()
        tag_rows = conn.execute(text("SELECT COALESCE(tags,'') AS tags FROM items")).all()
        days = get_last_n_days(5)
        min_d, max_d = days[0], days[-1]
        day_rows = conn.execute(text("""
            SELECT substr(created_at,1,10) AS d, COUNT(*) AS c
            FROM items
            WHERE substr(created_at,1,10) >= :min_d AND substr(created_at,1,10) <= :max_d
            GROUP BY substr(created_at,1,10) ORDER BY substr(created_at,1,10)
        """), {"min_d": min_d, "max_d": max_d}).all()

    recent = [row_to_dict(r) for r in recent_rows]
    label_counts = {l: 0 for l in ALLOWED_LABELS}
    unlabeled = 0
    for r in tag_rows:
        tags = [t.strip() for t in r._mapping["tags"].split(",") if t.strip()]
        if not tags:
            unlabeled += 1; continue
        matched = False
        for t in tags:
            if t in label_counts: label_counts[t] += 1; matched = True
        if not matched: unlabeled += 1

    total = sum(label_counts.values()) + unlabeled
    if total == 0:
        pie_labels, pie_values = ["No data"], [1]
    else:
        pie_labels = list(label_counts.keys()) + (["Unlabeled"] if unlabeled else [])
        pie_values = list(label_counts.values()) + ([unlabeled] if unlabeled else [])

    day_map = {d: 0 for d in days}
    for r in day_rows: day_map[r._mapping["d"]] = r._mapping["c"]

    return {
        "recent": recent,
        "pie_labels_json": json.dumps(pie_labels),
        "pie_values_json": json.dumps(pie_values),
        "line_labels_json": json.dumps(days),
        "line_values_json": json.dumps([day_map[d] for d in days]),
    }

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("index.html", ctx(request, compute_dashboard_stats()))

# ---- Reports CRUD ----
@app.get("/data", response_class=HTMLResponse)
def list_items(request: Request):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, title, description, tags, created_at, author, author_user_id, start_time, end_time
            FROM items ORDER BY id DESC
        """)).all()
    items = [row_to_dict(r) for r in rows]
    return templates.TemplateResponse("data.html", ctx(request, {"items": items}))

@app.get("/data/new", response_class=HTMLResponse)
def new_item_form(request: Request):
    u = current_user(request)
    if not u: return RedirectResponse(url="/auth/login", status_code=303)
    prof = get_profile(u["id"])
    return templates.TemplateResponse("new.html", ctx(request, {"current_user_name": prof.get("name") or u["username"]}))

@app.post("/data/new")
def create_item(
    request: Request,
    title: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    description: str = Form(""),
    labels: List[str] = Form([]),
):
    u = current_user(request)
    if not u: return RedirectResponse(url="/auth/login", status_code=303)
    prof = get_profile(u["id"])
    author = prof.get("name") or u["username"]
    labels = [l for l in labels if l in ALLOWED_LABELS]
    created_at = datetime.utcnow().isoformat()
    # validate
    sdt, edt = parse_dt(start_time, "start_time"), parse_dt(end_time, "end_time")
    if edt < sdt:
        return templates.TemplateResponse("new.html", ctx(request, {"current_user_name": author, "error": "End time must be after start time"}), status_code=400)

    with engine.begin() as conn:
        try:
            item_id = conn.execute(text("""
                INSERT INTO items (title, description, tags, created_at, author, author_user_id, start_time, end_time)
                VALUES (:t,:d,:g,:c,:a,:uid,:s,:e) RETURNING id
            """), {"t": title, "d": description, "g": ",".join(labels), "c": created_at, "a": author, "uid": u["id"], "s": start_time, "e": end_time}).scalar_one()
        except Exception:
            conn.execute(text("""
                INSERT INTO items (title, description, tags, created_at, author, author_user_id, start_time, end_time)
                VALUES (:t,:d,:g,:c,:a,:uid,:s,:e)
            """), {"t": title, "d": description, "g": ",".join(labels), "c": created_at, "a": author, "uid": u["id"], "s": start_time, "e": end_time})
            item_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()
        row = conn.execute(text("SELECT * FROM items WHERE id=:id"), {"id": item_id}).first()
    index_item_in_os(item_id, row_to_dict(row))
    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

@app.get("/data/{item_id}", response_class=HTMLResponse)
def view_item(request: Request, item_id: int):
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM items WHERE id = :id"), {"id": item_id}).first()
        if not row: raise HTTPException(status_code=404, detail="Not found")
        comments = conn.execute(text("""
            SELECT c.*, COALESCE(p.name, u.username) AS author_name
            FROM comments c JOIN users u ON u.id=c.user_id LEFT JOIN profiles p ON p.user_id=u.id
            WHERE c.item_id=:id ORDER BY c.id ASC
        """), {"id": item_id}).all()
    item = row_to_dict(row)
    comms = [row_to_dict(c) for c in comments]
    u = current_user(request)
    can_delete = bool(u and (u.get("is_admin") or u["id"] == item.get("author_user_id")))
    return templates.TemplateResponse("item.html", ctx(request, {"item": item, "comments": comms, "can_delete": can_delete}))

@app.get("/data/{item_id}/edit", response_class=HTMLResponse)
def edit_item_form(request: Request, item_id: int):
    u = current_user(request)
    if not u or not u.get("is_admin"):
        return RedirectResponse(url="/auth/login", status_code=303)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM items WHERE id=:id"), {"id": item_id}).first()
    return templates.TemplateResponse("edit.html", ctx(request, {"item": row_to_dict(row), "error": None}))

@app.post("/data/{item_id}/edit")
def edit_item_submit(request: Request, item_id: int,
    title: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    description: str = Form(""),
    labels: List[str] = Form([]),
):
    u = current_user(request)
    if not u or not u.get("is_admin"):
        return RedirectResponse(url="/auth/login", status_code=303)
    labels = [l for l in labels if l in ALLOWED_LABELS]
    sdt, edt = parse_dt(start_time, "start_time"), parse_dt(end_time, "end_time")
    if edt < sdt:
        return templates.TemplateResponse("edit.html", ctx(request, {"item": {"id": item_id, "title": title, "description": description, "tags": ",".join(labels), "start_time": start_time, "end_time": end_time}, "error": "End time must be after start time"}), status_code=400)
    with engine.begin() as conn:
        conn.execute(text("UPDATE items SET title=:t, description=:d, tags=:g, start_time=:s, end_time=:e WHERE id=:id"),
                     {"t": title, "d": description, "g": ",".join(labels), "s": start_time, "e": end_time, "id": item_id})
        row = conn.execute(text("SELECT * FROM items WHERE id=:id"), {"id": item_id}).first()
    index_item_in_os(item_id, row_to_dict(row))
    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

@app.post("/data/{item_id}/delete")
def delete_item(request: Request, item_id: int):
    u = current_user(request)
    if not u: return RedirectResponse(url="/auth/login", status_code=303)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT author_user_id FROM items WHERE id=:id"), {"id": item_id}).first()
    if not row: raise HTTPException(status_code=404, detail="Not found")
    owner = row._mapping["author_user_id"]
    if not (u.get("is_admin") or u["id"] == owner):
        raise HTTPException(status_code=403, detail="Not allowed")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM items WHERE id=:id"), {"id": item_id})
        conn.execute(text("DELETE FROM comments WHERE item_id=:id"), {"id": item_id})
    try: os_client.delete(index=OS_INDEX, id=str(item_id), ignore=[404])
    except Exception: pass
    return RedirectResponse(url="/data", status_code=303)

# ---- Comments ----
@app.post("/data/{item_id}/comment")
def add_comment(request: Request, item_id: int, content: str = Form(...)):
    u = current_user(request)
    if not u: return RedirectResponse(url="/auth/login", status_code=303)
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO comments (item_id, user_id, content, created_at) VALUES (:i,:u,:c,:t)"),
                     {"i": item_id, "u": u["id"], "c": content, "t": now})
    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

# ---- Modules ----
def normalize_label(label: str) -> str:
    cap = label.capitalize()
    if cap not in ALLOWED_LABELS: raise HTTPException(status_code=404, detail="Unknown module")
    return cap

@app.get("/module/{label}", response_class=HTMLResponse)
def module_page(request: Request, label: str):
    label = normalize_label(label)
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT id, title, author, created_at, start_time, end_time FROM items WHERE ','||tags||',' LIKE :needle ORDER BY id DESC"),
                            {"needle": f"%,{label},%"}).all()
    items = [row_to_dict(r) for r in rows]
    u = current_user(request)
    prof = get_profile(u["id"]) if u else None
    return templates.TemplateResponse("module.html", ctx(request, {"label": label, "items": items, "current_user_name": (prof.get('name') if prof else '')}))

@app.post("/module/{label}/new")
def module_create(
    request: Request,
    label: str,
    title: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    description: str = Form(""),
):
    u = current_user(request)
    if not u: return RedirectResponse(url="/auth/login", status_code=303)
    label = normalize_label(label)
    prof = get_profile(u["id"])
    author = prof.get("name") or u["username"]
    sdt, edt = parse_dt(start_time, "start_time"), parse_dt(end_time, "end_time")
    if edt < sdt:
        return templates.TemplateResponse("module.html", ctx(request, {"label": label, "items": [], "current_user_name": author, "error": "End time must be after start time"}), status_code=400)
    created_at = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        try:
            item_id = conn.execute(text("""
                INSERT INTO items (title, description, tags, created_at, author, author_user_id, start_time, end_time)
                VALUES (:t,:d,:g,:c,:a,:uid,:s,:e) RETURNING id
            """), {"t": title, "d": description, "g": label, "c": created_at, "a": author, "uid": u["id"], "s": start_time, "e": end_time}).scalar_one()
        except Exception:
            conn.execute(text("""
                INSERT INTO items (title, description, tags, created_at, author, author_user_id, start_time, end_time)
                VALUES (:t,:d,:g,:c,:a,:uid,:s,:e)
            """), {"t": title, "d": description, "g": label, "c": created_at, "a": author, "uid": u["id"], "s": start_time, "e": end_time})
            item_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()
        row = conn.execute(text("SELECT * FROM items WHERE id=:id"), {"id": item_id}).first()
    index_item_in_os(item_id, row_to_dict(row))
    return RedirectResponse(url=f"/data/{item_id}", status_code=303)

# ---- Search ----
@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: Optional[str] = None):
    results = []; err = None
    if q:
        try:
            resp = os_client.search(index=OS_INDEX, body={"query":{"multi_match":{"query":q,"fields":["title^2","description","tags","author"]}}})
            for hit in resp.get("hits",{}).get("hits",[]):
                src = hit.get("_source",{})
                results.append({"id": hit.get("_id"), "title": src.get("title"), "tags": ", ".join(src.get("tags",[]) or []), "created_at": src.get("created_at")})
        except Exception as e:
            err = str(e)
    return templates.TemplateResponse("search.html", ctx(request, {"q": q or "", "results": results, "error": err}))
