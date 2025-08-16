CSV Data Processor

This project downloads a large compressed CSV file, processes it efficiently in chunks using Pandas, performs data cleaning and transformation, and stores the result into a PostgreSQL database.

---

## Tech Stack

- Python 3.8+
- Pandas
- SQLAlchemy
- Requests
- PostgreSQL
- Logging

---

## Execution Steps

### 1. Clone the Repository

```bash
git clone https://github.com/vaishlaldinani/csv_data_processor.git
cd csv_data_processor
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Create PostgreSQL Database

```bash
createdb cdp_db
```

### 4. Create PostgreSQL User and Grant Access

```sql
-- Run in psql shell
CREATE USER cdp_user WITH PASSWORD 'cdp_pass';
GRANT ALL PRIVILEGES ON DATABASE cdp_db TO cdp_user;
```

### 5. Create Table from Schema

```bash
psql -U cdp_user -d cdp_db -f database.sql
```

### 6. Verify DB URI in Script

Ensure this line in `process_data.py` matches your DB setup:

```python
DB_URI = "postgresql+psycopg2://cdp_user:cdp_pass@localhost:5432/cdp_db"
```

### 7. Run the Data Processing Script

```bash
python process_data.py
```

### 8. Verify Inserted Data (Optional)

```sql
SELECT * FROM cdp_data LIMIT 10;
```

### 9. Check Logs (Optional)

```
logs/process.log
```

---

## Deliverables

- `process_data.py` – Data processing script
- `database.sql` – Database schema file
- `README.md` – Setup and execution instructions

---

## Error Handling & Logging

- Graceful failure handling using `try/except` blocks
- Logs written to `logs/process.log`

---

## Performance Optimizations

- Chunked reading with `pandas.read_csv(..., chunksize=...)`
- Memory-safe data processing pipeline
