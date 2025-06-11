**Database Field Naming Conventions Style Guide**

---

### 📚 Overview

This guide establishes standardized, semantic-based naming conventions for database fields across all tables in the system. It uses consistent abbreviations and structural patterns to promote clarity, maintainability, and ease of use.

---

### 🔢 General Naming Principles

* Use **snake\_case** for all table and field names.
* Avoid Hungarian notation (e.g., no `int`, `str` prefixes).
* Focus on **semantic meaning**, not data type.
* Use **abbreviated suffixes** to indicate field role (e.g., `_id`, `_nm`, `_cd`).
* Table names should be **singular** (e.g., `site`, `agency`).

---

### 🔹 Common Abbreviation Vocabulary

| Suffix | Meaning                | Example                |
| ------ | ---------------------- | ---------------------- |
| `_id`  | Primary or foreign key | `site_id`, `agency_id` |
| `_nm`  | Name                   | `project_nm`           |
| `_cd`  | Code                   | `status_cd`            |
| `_ind` | Boolean indicator      | `is_active_ind`        |
| `_tx`  | Free text              | `note_tx`              |
| `_ds`  | Description            | `error_ds`             |
| `_dt`  | Date                   | `start_dt`             |
| `_ts`  | Timestamp              | `updated_ts`           |
| `_cnt` | Count                  | `fish_cnt`             |
| `_amt` | Monetary amount        | `funding_amt`          |
| `_pct` | Percentage             | `completion_pct`       |
| `_no`  | Number (non-ID)        | `permit_no`            |

---

### 📝 Table Examples

**site**

* `site_id` (PK)
* `site_nm`
* `site_cd`
* `agency_cd` (FK to agency)
* `is_active_ind`
* `lat_dd`, `lon_dd` (for decimal degrees)

**agency**

* `agency_id` (PK)
* `agency_cd` 
* `agency_nm`
* `agency_type_cd`
* `is_federal_ind`

**project**

* `project_id` (PK)
* `project_nm`
* `site_id` (FK to site)
* `start_dt`, `end_dt`
* `funding_amt`
* `status_cd`

---

### ✨ Best Practices

* Always use `{entity}_id` for primary keys.
* Use `{referenced_entity}_id` or `{referenced_entity}_cd` for foreign keys.
* Prefix booleans with `is_` (e.g., `is_deleted_ind`).
* Encode units in field names when applicable (e.g., `_dd` for decimal degrees).
* Avoid using reserved words or ambiguous abbreviations.

---

### 📚 Optional Extensions

* Document lookup/reference tables using `_cd` fields (e.g., `status_cd`, `species_cd`).
* Use consistent suffixes for all temporal fields (`_dt`, `_ts`).
* Maintain a data dictionary mapping each field name to description, type, and constraints.

---

### 🌐 Versioning

* **v1.0** — Initial release of field naming convention.
* Maintain changelog as schema evolves.
