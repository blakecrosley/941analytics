# FastAPI Project Rules

## Stack Overview
- **Backend**: FastAPI (Python 3.11+)
- **Frontend**: HTMX + Alpine.js + Bootstrap 5
- **Templates**: Jinja2
- **Database**: SQLAlchemy 2.0+ (async)
- **Validation**: Pydantic v2

## Code Standards

### API Design
- RESTful conventions: `/resources`, `/resources/{id}`
- HTTP methods: GET (read), POST (create), PUT (replace), PATCH (update), DELETE
- Response format: `{"data": {}, "meta": {}, "errors": []}`
- Use Pydantic models for request/response validation

### Python Patterns
```python
# Type hints on all functions
async def get_item(item_id: int, db: AsyncSession = Depends(get_db)) -> ItemResponse:
    """Fetch item by ID. Returns 404 if not found."""
    item = await crud.get_item(db, item_id)
    if not item:
        raise HTTPException(404, "Item not found")
    return item
```

### Frontend Patterns
- HTMX for server interactions (not custom fetch)
- Alpine.js for client-only state (dropdowns, modals)
- Bootstrap 5 utilities for layout
- Plain CSS for custom styles (no Tailwind, no Sass)

### Database
- Async SQLAlchemy with connection pooling
- Migrations via Alembic
- Parameterized queries only (prevent SQL injection)

## Project Structure
```
app/
├── main.py           # FastAPI app, routes
├── database.py       # DB connection, session
├── models.py         # SQLAlchemy models
├── schemas.py        # Pydantic models
├── crud.py           # Database operations
└── config.py         # Settings

templates/
├── base.html         # Base layout
├── components/       # Reusable partials
└── pages/            # Page templates

static/
├── css/              # Stylesheets
└── js/               # JavaScript (minimal)
```

## Testing
- `pytest` with `pytest-asyncio`
- Test files: `tests/test_*.py`
- Run: `python -m pytest -v`
- Coverage: `--cov=app --cov-report=html`

## Common Commands
```bash
# Development
source venv/bin/activate
uvicorn app.main:app --reload --port 8000

# Testing
python -m pytest -v

# Database migrations
alembic upgrade head
alembic revision --autogenerate -m "description"
```

## Security Checklist
- [ ] HTTPS only (ATS in production)
- [ ] Input validation on all endpoints
- [ ] Parameterized database queries
- [ ] CORS configured with explicit origins
- [ ] Rate limiting enabled
- [ ] No secrets in code or logs
