# Frontend Enhancement PRDs for 941-Analytics

## Overview

The backend now collects `screen_width`, `screen_height`, and `language` data. The frontend has basic templates but needs enhancement to provide actionable insights.

**Current State:**
- `get_screen_sizes()` returns raw `{resolution, visits}` dicts
- `get_languages()` returns raw `{language, visits}` dicts
- Templates display simple tables without breakpoint grouping or language names

**Target State:**
- Screen sizes grouped by responsive breakpoints with summary
- Languages displayed with human-readable names
- Proper Pydantic models for type safety
- HTMX filter integration for drill-down

---

## PRD-F1: Backend Models Enhancement

**Goal:** Add proper Pydantic models for screen sizes and languages.

### Files to Modify
- `/src/analytics_941/core/models.py`

### Changes

Add these models after `BrowserStats`:

```python
class ScreenSizeStats(BaseModel):
    """Stats for screen resolution."""
    resolution: str  # "1920x1080"
    width: int
    height: int
    visits: int
    percentage: float
    breakpoint: str  # "mobile", "tablet", "desktop", "large"


class BreakpointStats(BaseModel):
    """Stats for a responsive breakpoint group."""
    breakpoint: str  # "mobile", "tablet", "desktop", "large"
    label: str  # "Mobile (<768px)"
    visits: int
    percentage: float
    resolutions: list[ScreenSizeStats] = []


class LanguageStats(BaseModel):
    """Stats for browser language."""
    code: str  # "en-US"
    name: str  # "English (United States)"
    base_language: str  # "en"
    visits: int
    percentage: float
```

### Acceptance Criteria
- [ ] Models added to `models.py`
- [ ] Models importable from `analytics_941.core.models`
- [ ] Type hints complete

---

## PRD-F2: Client Methods Enhancement

**Goal:** Enhance `get_screen_sizes()` and `get_languages()` to return typed models with computed fields.

### Files to Modify
- `/src/analytics_941/core/client.py`

### Changes

**Replace `get_screen_sizes()` (line 1158):**

```python
async def get_screen_sizes(
    self,
    start_date: date,
    end_date: date,
    limit: int = 20,
    filters: DashboardFilters | None = None,
) -> list[ScreenSizeStats]:
    """Get screen size breakdown with breakpoint classification."""

    filter_sql, filter_params = self._build_filter_sql(filters)

    results = await self._query(
        f"""
        SELECT
            screen_width,
            screen_height,
            COUNT(*) as visits
        FROM page_views
        WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
            AND is_bot = 0
            AND screen_width IS NOT NULL AND screen_height IS NOT NULL
            AND screen_width > 0 AND screen_height > 0
            {filter_sql}
        GROUP BY screen_width, screen_height
        ORDER BY visits DESC
        LIMIT ?
        """,
        [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
    )

    total = sum(r["visits"] for r in results)

    def classify_breakpoint(width: int) -> str:
        if width < 768:
            return "mobile"
        elif width < 1024:
            return "tablet"
        elif width < 1920:
            return "desktop"
        else:
            return "large"

    return [
        ScreenSizeStats(
            resolution=f"{r['screen_width']}x{r['screen_height']}",
            width=r["screen_width"],
            height=r["screen_height"],
            visits=r["visits"],
            percentage=round((r["visits"] / total) * 100, 1) if total > 0 else 0,
            breakpoint=classify_breakpoint(r["screen_width"]),
        )
        for r in results
    ]


async def get_screen_breakpoints(
    self,
    start_date: date,
    end_date: date,
    filters: DashboardFilters | None = None,
) -> list[BreakpointStats]:
    """Get screen sizes grouped by responsive breakpoints."""

    screen_sizes = await self.get_screen_sizes(start_date, end_date, limit=100, filters=filters)

    # Group by breakpoint
    breakpoint_labels = {
        "mobile": "Mobile (<768px)",
        "tablet": "Tablet (768-1024px)",
        "desktop": "Desktop (1024-1920px)",
        "large": "Large (>1920px)",
    }

    groups: dict[str, list[ScreenSizeStats]] = {}
    for ss in screen_sizes:
        bp = ss.breakpoint
        if bp not in groups:
            groups[bp] = []
        groups[bp].append(ss)

    total = sum(ss.visits for ss in screen_sizes)

    result = []
    for bp in ["mobile", "tablet", "desktop", "large"]:
        if bp in groups:
            group_visits = sum(ss.visits for ss in groups[bp])
            result.append(BreakpointStats(
                breakpoint=bp,
                label=breakpoint_labels[bp],
                visits=group_visits,
                percentage=round((group_visits / total) * 100, 1) if total > 0 else 0,
                resolutions=sorted(groups[bp], key=lambda x: x.visits, reverse=True)[:5],
            ))

    return result
```

**Replace `get_languages()` (line 1129):**

```python
# Language name lookup (add at module level)
LANGUAGE_NAMES: dict[str, str] = {
    "en": "English",
    "en-US": "English (US)",
    "en-GB": "English (UK)",
    "es": "Spanish",
    "es-ES": "Spanish (Spain)",
    "es-MX": "Spanish (Mexico)",
    "fr": "French",
    "fr-FR": "French (France)",
    "de": "German",
    "de-DE": "German (Germany)",
    "it": "Italian",
    "pt": "Portuguese",
    "pt-BR": "Portuguese (Brazil)",
    "ja": "Japanese",
    "ko": "Korean",
    "zh": "Chinese",
    "zh-CN": "Chinese (Simplified)",
    "zh-TW": "Chinese (Traditional)",
    "ru": "Russian",
    "ar": "Arabic",
    "hi": "Hindi",
    "nl": "Dutch",
    "pl": "Polish",
    "sv": "Swedish",
    "da": "Danish",
    "fi": "Finnish",
    "no": "Norwegian",
    "tr": "Turkish",
    "he": "Hebrew",
    "th": "Thai",
    "vi": "Vietnamese",
    "id": "Indonesian",
    "ms": "Malay",
    "uk": "Ukrainian",
    "cs": "Czech",
    "el": "Greek",
    "hu": "Hungarian",
    "ro": "Romanian",
    "sk": "Slovak",
    "bg": "Bulgarian",
    "hr": "Croatian",
    "sl": "Slovenian",
    "et": "Estonian",
    "lv": "Latvian",
    "lt": "Lithuanian",
}


def get_language_name(code: str) -> str:
    """Convert language code to human-readable name."""
    if code in LANGUAGE_NAMES:
        return LANGUAGE_NAMES[code]
    # Try base language
    base = code.split("-")[0].lower()
    if base in LANGUAGE_NAMES:
        return f"{LANGUAGE_NAMES[base]} ({code.split('-')[-1].upper()})"
    return code


async def get_languages(
    self,
    start_date: date,
    end_date: date,
    limit: int = 10,
    filters: DashboardFilters | None = None,
) -> list[LanguageStats]:
    """Get language breakdown with human-readable names."""

    filter_sql, filter_params = self._build_filter_sql(filters)

    results = await self._query(
        f"""
        SELECT
            language,
            COUNT(*) as visits
        FROM page_views
        WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
            AND is_bot = 0 AND language != '' AND language IS NOT NULL
            {filter_sql}
        GROUP BY language
        ORDER BY visits DESC
        LIMIT ?
        """,
        [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
    )

    total = sum(r["visits"] for r in results)

    return [
        LanguageStats(
            code=r["language"],
            name=get_language_name(r["language"]),
            base_language=r["language"].split("-")[0].lower(),
            visits=r["visits"],
            percentage=round((r["visits"] / total) * 100, 1) if total > 0 else 0,
        )
        for r in results
    ]
```

### Acceptance Criteria
- [ ] `get_screen_sizes()` returns `list[ScreenSizeStats]`
- [ ] `get_screen_breakpoints()` returns grouped data
- [ ] `get_languages()` returns `list[LanguageStats]` with names
- [ ] All methods handle empty results gracefully

---

## PRD-F3: Dashboard Route Integration

**Goal:** Update technology_page route to pass enhanced data to templates.

### Files to Modify
- `/src/analytics_941/routes/dashboard.py`

### Changes

Find the `technology_page` function and update to fetch breakpoint data:

```python
# In the _parallel_queries call for technology page, add:
screen_breakpoints=client.get_screen_breakpoints(start_date, end_date, filters),
screen_sizes=client.get_screen_sizes(start_date, end_date, limit=20, filters=filters),
languages=client.get_languages(start_date, end_date, limit=15, filters=filters),
```

Update template context to include:
```python
{
    "screen_breakpoints": data["screen_breakpoints"],
    "screen_sizes": data["screen_sizes"],
    "languages": data["languages"],
    # ... existing fields
}
```

### Acceptance Criteria
- [ ] Route fetches `screen_breakpoints`, `screen_sizes`, `languages`
- [ ] Template context includes all three
- [ ] No N+1 queries (parallel fetch)

---

## PRD-F4: Screen Sizes Template Enhancement

**Goal:** Add breakpoint summary section above detailed resolution table.

### Files to Modify
- `/src/analytics_941/templates/partials/technology_content.html`

### Changes

Replace the Screen Sizes section (lines 154-192) with:

```jinja2
{# Screen Sizes - Enhanced with Breakpoint Summary #}
<div class="analytics-section">
    <div class="analytics-section__header">
        <span class="analytics-section__title">Screen Sizes</span>
    </div>
    <div class="analytics-section__content">
        {# Breakpoint Summary #}
        {% if screen_breakpoints|length > 0 %}
        <div class="analytics-breakpoints">
            {% for bp in screen_breakpoints %}
            <div class="analytics-breakpoint">
                <div class="analytics-breakpoint__label">{{ bp.label }}</div>
                <div class="analytics-breakpoint__bar">
                    <div class="analytics-breakpoint__fill" style="width: {{ bp.percentage }}%;"></div>
                </div>
                <div class="analytics-breakpoint__value">{{ "{:.1f}".format(bp.percentage) }}%</div>
            </div>
            {% endfor %}
        </div>
        <hr class="analytics-divider">
        {% endif %}

        {# Detailed Resolution Table #}
        {% if screen_sizes|length > 0 %}
        {% set total_screens = screen_sizes|map(attribute='visits')|sum %}
        <table class="analytics-table">
            <thead>
                <tr>
                    <th>Resolution</th>
                    <th>Type</th>
                    <th class="analytics-text-right">Visits</th>
                    <th class="analytics-text-right">%</th>
                </tr>
            </thead>
            <tbody>
                {% for screen in screen_sizes[:10] %}
                <tr>
                    <td>
                        {{ screen.resolution }}
                        <div class="analytics-table__bar">
                            <div class="analytics-table__bar-fill" style="width: {{ screen.percentage|int }}%;"></div>
                        </div>
                    </td>
                    <td>
                        <span class="analytics-badge analytics-badge--{{ screen.breakpoint }}">
                            {{ screen.breakpoint|capitalize }}
                        </span>
                    </td>
                    <td class="analytics-text-right analytics-table__number">{{ "{:,}".format(screen.visits) }}</td>
                    <td class="analytics-text-right analytics-table__number">{{ "{:.1f}".format(screen.percentage) }}%</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <div class="analytics-empty">
            <div class="analytics-empty__icon">📐</div>
            <p>No screen size data</p>
        </div>
        {% endif %}
    </div>
</div>
```

### Acceptance Criteria
- [ ] Breakpoint summary bars display above table
- [ ] Table shows breakpoint badge per resolution
- [ ] Empty state handled
- [ ] Percentages calculated correctly

---

## PRD-F5: Languages Template Enhancement

**Goal:** Show language names with codes, add filter links.

### Files to Modify
- `/src/analytics_941/templates/partials/technology_content.html`

### Changes

Replace the Languages section (lines 194-232) with:

```jinja2
{# Languages - Enhanced with Names and Filtering #}
<div class="analytics-section">
    <div class="analytics-section__header">
        <span class="analytics-section__title">Languages</span>
    </div>
    <div class="analytics-section__content analytics-section__content--flush">
        {% if languages|length > 0 %}
        <table class="analytics-table">
            <thead>
                <tr>
                    <th>Language</th>
                    <th class="analytics-text-right">Visits</th>
                    <th class="analytics-text-right">%</th>
                </tr>
            </thead>
            <tbody>
                {% for lang in languages[:10] %}
                <tr>
                    <td>
                        <a href="?language={{ lang.code|urlencode }}"
                           hx-get="./partials/technology?language={{ lang.code|urlencode }}"
                           hx-target="#main-content"
                           hx-push-url="?language={{ lang.code|urlencode }}"
                           style="text-decoration: none; color: var(--941-color-text-muted);">
                            🌐 {{ lang.name }}
                            <br>
                            <small class="analytics-text-muted">{{ lang.code }}</small>
                        </a>
                        <div class="analytics-table__bar">
                            <div class="analytics-table__bar-fill" style="width: {{ lang.percentage|int }}%;"></div>
                        </div>
                    </td>
                    <td class="analytics-text-right analytics-table__number">{{ "{:,}".format(lang.visits) }}</td>
                    <td class="analytics-text-right analytics-table__number">{{ "{:.1f}".format(lang.percentage) }}%</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <div class="analytics-empty">
            <div class="analytics-empty__icon">🌍</div>
            <p>No language data</p>
        </div>
        {% endif %}
    </div>
</div>
```

### Acceptance Criteria
- [ ] Language names displayed (not just codes)
- [ ] Code shown as secondary text
- [ ] HTMX filter links work
- [ ] Empty state handled

---

## PRD-F6: CSS Styling for New Components

**Goal:** Add CSS for breakpoint bars and badges.

### Files to Modify
- `/src/analytics_941/static/css/dashboard.css`

### Changes

Add to dashboard.css:

```css
/* Breakpoint Summary Bars */
.analytics-breakpoints {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    padding: 1rem;
}

.analytics-breakpoint {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.analytics-breakpoint__label {
    flex: 0 0 150px;
    font-size: 0.875rem;
    color: var(--941-color-text-muted);
}

.analytics-breakpoint__bar {
    flex: 1;
    height: 24px;
    background: var(--941-color-bg-tertiary);
    border-radius: 4px;
    overflow: hidden;
}

.analytics-breakpoint__fill {
    height: 100%;
    background: var(--941-color-primary);
    border-radius: 4px;
    transition: width 0.3s ease;
}

.analytics-breakpoint__value {
    flex: 0 0 50px;
    text-align: right;
    font-size: 0.875rem;
    font-weight: 600;
    color: var(--941-color-text);
}

/* Breakpoint Badges */
.analytics-badge {
    display: inline-block;
    padding: 0.125rem 0.5rem;
    font-size: 0.75rem;
    font-weight: 500;
    border-radius: 4px;
    text-transform: capitalize;
}

.analytics-badge--mobile {
    background: rgba(59, 130, 246, 0.1);
    color: #3b82f6;
}

.analytics-badge--tablet {
    background: rgba(168, 85, 247, 0.1);
    color: #a855f7;
}

.analytics-badge--desktop {
    background: rgba(34, 197, 94, 0.1);
    color: #22c55e;
}

.analytics-badge--large {
    background: rgba(249, 115, 22, 0.1);
    color: #f97316;
}

/* Divider */
.analytics-divider {
    border: none;
    border-top: 1px solid var(--941-color-border);
    margin: 1rem 0;
}
```

### Acceptance Criteria
- [ ] Breakpoint bars styled with colors
- [ ] Badges color-coded by device type
- [ ] Responsive on mobile
- [ ] Matches existing dashboard aesthetic

---

## Implementation Order

1. **PRD-F1** - Models (foundation, no dependencies)
2. **PRD-F2** - Client methods (depends on F1)
3. **PRD-F6** - CSS (no dependencies, can be parallel)
4. **PRD-F3** - Route integration (depends on F2)
5. **PRD-F4** - Screen sizes template (depends on F3)
6. **PRD-F5** - Languages template (depends on F3)

## Testing

After each PRD:
1. Visit `https://blakecrosley.com/admin/analytics`
2. Click "Technology" tab
3. Verify new sections render
4. Check browser console for errors
5. Test HTMX filter clicks

## Rollback

If issues occur:
1. Revert to previous `get_languages()` / `get_screen_sizes()` returning dicts
2. Templates handle both typed models and dicts gracefully
