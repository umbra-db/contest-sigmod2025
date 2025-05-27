#!/usr/bin/env python3
# %%
import duckdb, psycopg2, psycopg2.pool, traceback, threading
import json, glob, os, subprocess, re, random
from concurrent.futures import ThreadPoolExecutor

# %%
#def compute_domains():
#    with duckdb.connect(database='imdb.db', read_only=True, config={'access_mode': 'read_only'}) as conn:
#        os.makedirs("domain", exist_ok=True)
#        with open("schema.json") as f:
#            schema = json.load(f)
#        for tbl in schema:
#            tn = tbl['table_name']
#            for col in tbl['columns']:
#                cn = col['name']
#                print(f"Computing domain for {tn}.{cn}")
#                res = conn.execute(f"SELECT DISTINCT {cn} FROM {tn} ORDER BY {cn}")
#                domain = [r[0] for r in res.fetchall()]
#                col["domain"] = domain
#    with open("schema-domain.json", "w") as f:
#        json.dump(schema, f)
#compute_domains()

# %%

active_duckdb = None

domain_cache = {}
def prepare_domain(table, column):
    global active_duckdb
    if (table, column) in domain_cache:
        return domain_cache[(table, column)]
    def logic(conn):
        #print(f"Computing domain for {table}.{column}")
        res = conn.execute(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL ORDER BY {column}")
        domain = [r[0] for r in res.fetchall()]
        # compute safe domain
        if len(domain) > 0 and isinstance(domain[0], str):
            domain = [d for d in domain if d is None or "'" not in d]
        domain_cache[(table, column)] = domain
        #print(f"Domain for {table}.{column} is {len(domain)}")
        return domain
    if active_duckdb is None:
        print("Connecting to duckdb")
        with duckdb.connect(database='imdb.db', read_only=True, config={'access_mode': 'read_only'}) as conn:
            return logic(conn)
    else:
        return logic(active_duckdb)

# %%
def gen_templates():
    with open("schema.json") as f:
        schema = json.load(f)

    templates = {}
    for f in glob.glob("job/*a.sql"):
        if "schema" in f:
            continue
        with open(f) as ff:
            q = ff.read()
        q = q.strip()
        q = q.strip(";")

        selectPos = q.find("SELECT")
        fromPos = q.find("FROM")
        wherePos = q.find("WHERE")

        selectPart = q[selectPos + 6:fromPos].split(",")
        fromPart = q[fromPos + 4:wherePos].split(",")
        wherePart = re.split(r'\band\b', q[wherePos + 5:], flags=re.IGNORECASE)


        def split_select(x):
            # parse min(a) as b to [a, b]
            if "AS" in x:
                parts = [c.strip() for c in x.strip().split("AS")]
            else:
                parts = [c.strip() for c in x.strip().split(" ")]
            parts[0] = parts[0][4:-1]
            return parts
        selectPart = [split_select(x) for x in selectPart]

        def split_from(x):
            if "AS" in x:
                res = [c.strip() for c in x.strip().split("AS")]
            else:
                res = [c.strip() for c in x.strip().split(" ")]
            return {"table": res[0], "alias": res[1]}
        fromPart = [split_from(x) for x in fromPart]

        joinPredicates = []
        for p in wherePart:
            p = p.strip()
            porg = p
            # if predicate looks like "a.b = c.d" not "a.b = 1"
            if "=" not in p:
                continue
            p = p.split("=")
            if len(p) != 2:
                continue
            if "." not in p[0] or "." not in p[1]:
                continue
            joinPredicates.append(porg)

        columns = []
        aliases = {}
        for ff in fromPart:
            tbl = ff["table"]
            alias = ff["alias"]
            aliases[alias] = tbl
            t = schema[tbl]
            for c, v in t.items():
                columns.append({"alias": f"{alias}.{c}", "table": tbl, "column": c})

        sels = []
        for n, r in selectPart:
            a, c = [v.strip() for v in n.split(".")]
            sels.append({"attr": n, "table": aliases[a], "alias": a, "column": c, "rename": r})



        res = {
            "select": sels,
            "from": fromPart,
            "join": joinPredicates,
            "columns": columns
        }

        templateName = os.path.basename(f)[:-5]
        templates[templateName] = res

    with open("templates.json", "w") as f:
        json.dump(templates, f, indent=2)
#gen_templates()


# %%

with open("schema.json") as f:
    schema = json.load(f)
with open("templates.json") as f:
    templates = json.load(f)

# %%

def format_constant(const):
    if const is None:
        return "NULL"
    if isinstance(const, str):
        return f"'{const}'"
    return str(const)

def pick_constant(table, column):
    domain = prepare_domain(table, column)
    if len(domain) == 0:
        return None
    return format_constant(random.choice(domain))

def pick_constant_for_like(table, column):
    domain = prepare_domain(table, column)
    if len(domain) == 0:
        return None
    const = random.choice(domain)
    if const is None:
        return format_constant(const)
    def format_result(const):
        return f"'%{const}%'"
    if "-" in const:
        return format_result(random.choice(const.split("-")))
    if " " in const:
        return format_result(random.choice(const.split(" ")))
    # random substring
    r0 = random.randint(0, len(const) - 1)
    r1 = random.randint(r0 + 1, len(const))
    return format_result(const[r0:r1])

def gen_eq(name, table, column):
    c = pick_constant(table, column)
    if c is None:
        return None
    return f"{name} = {c}"
def gen_lt(name, table, column):
    c = pick_constant(table, column)
    if c is None:
        return None
    return f"{name} < {c}"
def gen_gt(name, table, column):
    c = pick_constant(table, column)
    if c is None:
        return None
    return f"{name} > {c}"
def gen_between(name, table, column):
    c1 = pick_constant(table, column)
    c2 = pick_constant(table, column)
    if c1 is None or c2 is None:
        return None
    if c1 > c2:
        c1, c2 = c2, c1
    return f"{name} BETWEEN {c1} AND {c2}"
def gen_in(name, table, column):
    domain = prepare_domain(table, column)
    if len(domain) == 0:
        return None
    numValues = random.randint(1, min(10, len(domain)))
    values = [format_constant(v) for v in random.sample(domain, numValues)]
    values.sort()
    return f"{name} IN ({', '.join(values)})"
def gen_like(name, table, column):
    c = pick_constant_for_like(table, column)
    if c is None:
        return None
    return f"{name} LIKE {c}"
def gen_is_not_null(name, table, column):
    if schema[table][column]["not_null"]:
        return None
    return f"{name} IS NOT NULL"
gens = {
    "text": [gen_eq, gen_lt, gen_gt, gen_in, gen_is_not_null, gen_like],
    "integer": [gen_eq, gen_lt, gen_gt, gen_is_not_null, gen_in]
}

def gen_predicate(name, table, column):
    type = schema[table][column]["type"]
    return random.choice(gens[type])(name, table, column)

def gen_predicates(template):
    predicates = []
    numPredicates = random.randint(0, min(8, len(template["columns"])))
    for col in random.sample(template["columns"], numPredicates):
        p = gen_predicate(col["alias"], col["table"], col["column"])
        if p is not None:
            predicates.append(p)
    return " AND ".join(predicates)

def query_to_str(template, predicates = None, count_star = False):
    query = ""
    query += "SELECT "
    if count_star:
        query += "COUNT(*)"
    else:
        query += ", ".join([f"min({s['attr']}) AS {s['rename']}" for s in template["select"]])
    query += "\nFROM "
    query += ", ".join([f"{f['table']} AS {f['alias']}" for f in template["from"]])
    query += "\nWHERE "
    query += " AND ".join(template["join"])
    if predicates is not None and len(predicates) > 0:
        query += "\nAND "
        query += predicates
    query += ";"
    return query


template_names = list(templates.keys())

def gen_query():
    name = random.choice(template_names)
    template = templates[name]
    preds = gen_predicates(template)
    return name, query_to_str(template, preds, False), query_to_str(template, preds, True)

print(gen_query())

# %%
print("Connecting to postgres")
try:
    pool = psycopg2.pool.ThreadedConnectionPool(1, 8,f"dbname=postgres user=postgres password=123456 host=localhost options='-c statement_timeout=10000'", connect_timeout=3)
except Exception as e:
    print("Retrying with remote server")
    pool = psycopg2.pool.ThreadedConnectionPool(1, 8,f"dbname=job user=jobuser password=kaiVa0fa3Chah3oo host=pg13-db.captain.birler.co options='-c statement_timeout=10000'", connect_timeout=3)
print("Connected to postgres")

def has_nested_loop(obj):
    if isinstance(obj, dict):
        if obj.get("Node Type") == "Nested Loop":
            return True
        return any(has_nested_loop(v) for v in obj.values())
    elif isinstance(obj, list):
        return any(has_nested_loop(item) for item in obj)
    return False

def get_explain(query):
    # Get the explain plan from PostgreSQL
    conn = pool.getconn()
    cursor = conn.cursor()
    cursor.execute(f"set enable_nestloop = off;")
    cursor.execute(f"set enable_bitmapscan = off;")
    cursor.execute(f"set enable_indexscan = off;")
    try:
        cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
    except Exception as e:
        print(f"Error executing EXPLAIN: {e}")
        print(query)
        traceback.print_exc()
        cursor.close()
        pool.putconn(conn)
        return None
    explain_output = cursor.fetchall()
    cursor.close()
    pool.putconn(conn)
    res = explain_output[0][0][0]
    if has_nested_loop(res):
        return None
    return res

print("Testing explain")
print(get_explain("SELECT * FROM title WHERE title = 'The Matrix';"))

# %%
def test_in_duckdb(query):
    try:
        args = ["duckdb", "imdb.db", "-csv", "-c", f"set memory_limit='10GB'; set temp_directory='';{query}"]
        result = subprocess.run(args, capture_output=True, text=True, timeout=3, start_new_session=True)

        # Check if DuckDB returned an error or was killed.
        if result.returncode != 0:
            # do not print oom as we expect it
            if "Out of Memory Error" not in result.stderr:
                print("DuckDB shell process failed with return code", result.returncode)
                print("Error output:", result.stderr)
            return False

        last = [l for l in result.stdout.split("\n") if l.strip() != ""][-1]
        # Skip if query is too large
        return int(last) < 1e7

    except subprocess.TimeoutExpired:
        #print("DuckDB query timed out.")
        return False

    except Exception as e:
        print("An error occurred when running DuckDB shell:", e)
        return False

print("Testing in DuckDB")
test_in_duckdb("SELECT count(*) FROM title WHERE title = 'The Matrix'")

# %%

if __name__ == "__main__":
    print("Starting query generation")
    os.makedirs("queries", exist_ok=True)

    query_batch = 1000
    total_queries = 0
    error_lock = threading.Lock()
    log_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=8) as executor:
        while True:
            queries = []
            print(f"\rGenerating queries 0/{query_batch}", end="")
            with duckdb.connect(database='imdb.db', read_only=True, config={'access_mode': 'read_only'}) as conn:
                active_duckdb = conn
                for i in range(query_batch):
                    t, q, qstar = gen_query()

                    # generate random 4 char id for query
                    name = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=4))
                    with open(f"queries/{t}_{name}.sql", "w") as f:
                        f.write(q)
                    queries.append((t, name, q, qstar))
                    print(f"\rGenerating queries {i+1}/{query_batch}", end="")
                active_duckdb = None
            print()

            explain_lock = threading.Lock()
            global numExplained
            numExplained = 0
            print(f"\rExplained {numExplained}/{len(queries)}", end="")
            def handleExplain(qinfo):
                global numExplained
                t, name, q, qstar = qinfo
                e = get_explain(q)
                if e is not None:
                    with open(f"queries/{t}_{name}.json", "w") as f:
                        json.dump({
                            "sql_directory": f"queries",
                            "names": [f"{t}_{name}"],
                            "plans": [e],
                        }, f, indent=4)
                with explain_lock:
                    numExplained += 1
                    print(f"\rExplained {numExplained}/{len(queries)}", end="")
                return t, name, q, qstar, e

            queries = list(executor.map(handleExplain, queries))
            queries = [q for q in queries if q[4] is not None]
            print()

            q2 = []
            print(f"\rTesting with duckdb 0/{len(queries)}", end="")
            for i, q in enumerate(queries):
                if test_in_duckdb(q[3]):
                    q2.append(q)
                print(f"\rTesting with duckdb {i+1}/{len(queries)}", end="")
            queries = q2
            print()
            if len(queries) == 0:
                continue

            queries = [(t, name, q, e) for t, name, q, qstar, e in queries if e is not None]
            total_queries += len(queries)
            print(f"Generated {len(queries)} queries. Total {total_queries} queries.")

            # Generate a merged file
            if len(queries) > 1:
                multiName = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=4))
                merged_file = f"queries/multi_{multiName}.json"
                with open(merged_file, "w") as f:
                    json.dump({
                        "sql_directory": f"queries",
                        "names": [f"{t}_{name}" for t, name, q, e in queries],
                        "plans": [e for t, name, q, e in queries],
                    }, f, indent=4)
                print("Generated merged file:", merged_file)
            else:
                merged_file = f"queries/{queries[0][0]}_{queries[0][1]}.json"

            print("Executing queries")

            def runQuery(plans_file):
                with open("querygen.log", "a") as log_file:
                    log_file.write(f"Executing: {plans_file}\n")

                # Execute cmake-build-relwithassert/run queries/{t}_{i}.json
                # Check the return value
                env = os.environ.copy()
                #env["SERIALIZE"] = "0"
                res = subprocess.run(["cmake-build-relwithassert/run", f"{plans_file}"], check=False, env=env, start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                print(f"Executed: {plans_file} with result code: {res.returncode}")

                if res.returncode != 0:
                    print(f"Error executing plans: {plans_file}")
                    with error_lock:
                        with open("errors.log", "a") as err_file:
                            err_file.write(f"Error executing plans {plans_file} with returncode {res.returncode}\n")
                            if res.stdout:
                                lines = res.stdout.decode().split("\n")
                                lines = [l for l in lines if "false" in l]
                                for l in lines:
                                    err_file.write("> " + l + "\n")
                            if res.stderr:
                                lines = res.stderr.decode().split("\n")
                                # print the last couple of lines
                                lines = [l for l in lines if l.strip() != ""]
                                lines = lines[-3:]
                                for l in lines:
                                    err_file.write("# " + l + "\n")

                with open("querygen.log", "a") as log_file:
                    log_file.write(f"Executed: {plans_file} with return code {res.returncode}\n")

                return res.returncode

            codes = [runQuery(merged_file)]
            success = all(code == 0 for code in codes)

            print(f"Executed {len(queries)} queries with all {'SUCCESS' if success else 'FAIL'}")

# %%
