# Pesapal RDBMS

Pesapal RDBMS is a **lightweight relational database management system implemented entirely in Python**. It is designed as an educational and showcase project that demonstrates core RDBMS concepts such as schema definition, data integrity, indexing, querying, and persistence — all built from first principles.

The project intentionally focuses on clarity, correctness, and ownership of logic rather than production-scale optimizations.

---

## Key Capabilities

- **Declarative schemas**
  Define tables with named columns, basic data types (`int`, `str`, `float`, `bool`), and constraints.

- **CRUD operations**
  Create, read, update, and delete records using a simple SQL-like interface.

- **Keys and constraints**
  Support for primary keys, unique constraints, and constraint validation.

- **Indexing**
  Basic indexing to improve lookups and enforce uniqueness rules.

- **Joins**
  Simple join functionality to combine related tables.

- **Interactive REPL**
  A command-line interface for issuing SQL-like statements interactively.

- **Persistence**
  Database state is stored in JSON files and restored between runs.

- **Web demonstration**
  A small web application that demonstrates real-world CRUD usage against the database engine.

---

## Repository Structure

```
.
├── core.py          # Core Database and Table logic, constraints, indexing, persistence
├── parser.py        # SQL-like command parsing and execution layer
├── repl.py          # Interactive REPL for issuing commands
├── tests.py         # Automated test suite covering core and optional features
├── web/
│   ├── app.py       # Minimal web application demonstrating CRUD usage
│   ├── templates/
│   │   └── index.html
│   └── static/      # Frontend assets
└── *.json           # Local persistence files (ignored by version control)
```

---

## Requirements

- Python **3.10+** (3.8+ may also work)
- Recommended: use a virtual environment
- Optional (for the web demo only):

  - `Flask`
  - `flask-cors`

---

## Setup

Create and activate a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
.\.venv\Scripts\activate         # Windows
```

Install optional dependencies for the web demo:

```bash
pip install Flask flask-cors
```

---

## Interactive REPL

The REPL allows you to interact with the database engine using SQL-like commands.

### Start the REPL

```bash
python repl.py
```

### Example Commands

```sql
CREATE TABLE users (id int, full_name str, email str) PRIMARY KEY(id);
INSERT INTO users (id, full_name, email) VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users;
UPDATE users SET full_name = 'Alice B' WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

The supported syntax intentionally covers a **small, well-defined subset** of SQL-style commands. Refer to `parser.py` for exact capabilities and limitations.

---

## Web Demonstration

A minimal web application is included to demonstrate how the database can be used in a real application context.

### Run the Web App

```bash
python web/app.py
```

Then open your browser at:

```
http://127.0.0.1:5000/
```

### API Endpoints

The frontend communicates with the backend using simple JSON APIs:

- `GET /api/users` — list all users
- `POST /api/users` — create a user
- `GET /api/users/<id>` — retrieve a single user
- `PUT /api/users/<id>` — update a user
- `DELETE /api/users/<id>` — delete a user
- `GET /api/stats` — basic statistics

The web app uses the **same core database and parser layers** as the REPL.

---

## Persistence

- Database state is stored in local JSON files
- Data is written when `db.save()` is invoked
- Files are intended for local use and should not be committed to source control

Persistence ensures that data survives application restarts while keeping the implementation simple and transparent.

---

## Testing

An automated test suite validates both core functionality and optional enhancements.

Run tests with:

```bash
python tests.py
```

### Test Coverage Includes

- Schema definition and validation
- CRUD operations
- Primary and unique constraints
- Index behavior and maintenance
- Join correctness
- Persistence and reload behavior
- Error handling and edge cases

---

## Extending the Project

Potential enhancements include:

- Additional data types and validators
- More expressive query syntax
- Transaction and rollback support
- Improved join strategies
- Query optimization
- Authentication and authorization layers
- Expanded web frontend functionality

---

## Attribution & Ownership

This project was built as a **from-scratch implementation** to demonstrate understanding of relational database fundamentals. External libraries are used only where explicitly stated (for example, the web framework). Any future reuse of third-party code should be clearly credited.

---

## License

No license file is currently included. Add a suitable license if you plan to distribute or reuse this project.

---

## Author Notes

Pesapal RDBMS is intended as both a learning exercise and a demonstration of systems-level thinking. Feedback and extensions are welcome.
