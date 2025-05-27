import duckdb
import sys

with duckdb.connect(database='imdb2.db', read_only=True, config={'access_mode': 'read_only'}) as conn:
    conn.execute("set memory_limit='10GB';")
    conn.execute("set temp_directory='';")
    while sys.stdin:
        query = sys.stdin.readline().strip()
        print(f"Received query: {query}", file=sys.stderr)
        if not query:
            break
        try:
            result = conn.execute(query).fetchall()
            print(f"Executed query {query} with result {result}", file=sys.stderr)
            print(result[0][0])
            sys.stdout.flush()
        except Exception as e:
            print(f"Executed query {query} with error: {e}", file=sys.stderr)
            print(-1)
            sys.stdout.flush()
            continue

sys.stdin.close()