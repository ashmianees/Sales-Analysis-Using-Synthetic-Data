#!/usr/bin/env python3
"""
seed_textiles_full.py
Creates schema, inserts master data with category–brand mapping (as provided),
and generates multi-year sales transactions.

Run:
    python seed_textiles_full.py

Output:
    - A SQLite DB file named `textiles.db` in the same folder.
"""

import sqlite3
import random
import requests
from datetime import datetime, timedelta
from collections import defaultdict

DB = "textiles.db"
SEED = 42
random.seed(SEED)

# ---------------------------
# Config
# ---------------------------
NUM_PRODUCTS = 250
START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 12, 31)
AVG_TRANSACTIONS_PER_DAY = 120
WEEKEND_MULTIPLIER = 1.6      # Weekend sales higher
FESTIVAL_MULTIPLIER = 2.2     # Holiday/Festival sales higher
MAX_ITEMS_PER_BASKET = 4
CO_OCCURRENCE_LINKS = 0.06
BATCH_COMMIT = 3000

# ---------------------------
# Helper: holiday fetch (Calendarific)
# ---------------------------
def fetch_holidays_calendarific(years, country_code="IN"):
    API_KEY = "4O92vYiKIXAhc0XqLEHYyDyXsqMNg3cp"  # replace with your key
    holidays = {}
    base = "https://calendarific.com/api/v2/holidays"
    for y in years:
        url = f"{base}?api_key={API_KEY}&country={country_code}&year={y}"
        try:
            r = requests.get(url, timeout=12)
            r.raise_for_status()
            j = r.json()
            if "response" in j:
                for h in j["response"]["holidays"]:
                    date = h["date"]["iso"].split("T")[0]
                    name = h["name"]
                    holidays[date] = name
            print(f"Fetched {len(holidays)} holidays for {y} via Calendarific")
        except Exception as e:
            print("Error fetching Calendarific holidays:", e)
    return holidays

# ---------------------------
# Schema creation
# ---------------------------
def create_schema(conn):
    cur = conn.cursor()
    tables = [
        "product_styles", "product_variants", "products",
        "categories", "brands", "materials", "suppliers",
        "collections", "styles", "colors", "sizes", "sales", "holidays"
    ]
    for t in tables:
        cur.execute(f"DROP TABLE IF EXISTS {t}")

    cur.execute("""CREATE TABLE categories (category_id INTEGER PRIMARY KEY AUTOINCREMENT, category_name TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE brands (brand_id INTEGER PRIMARY KEY AUTOINCREMENT, brand_name TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE materials (material_id INTEGER PRIMARY KEY AUTOINCREMENT, material_name TEXT NOT NULL)""")
    
    # Updated suppliers table with brand_name and contact_info
    cur.execute("""CREATE TABLE suppliers (
        supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_name TEXT NOT NULL,
        brand_name TEXT,
        contact_info TEXT
    )""")
    
    cur.execute("""CREATE TABLE collections (collection_id INTEGER PRIMARY KEY AUTOINCREMENT, collection_name TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE styles (style_id INTEGER PRIMARY KEY AUTOINCREMENT, style_name TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_name TEXT NOT NULL,
        category_id INTEGER,
        brand_id INTEGER,
        material_id INTEGER,
        supplier_id INTEGER,
        collection_id INTEGER,
        brought_unit_price DECIMAL(10,2),
        current_price DECIMAL(10,2),
        offer DECIMAL(5,2),
        price_after_offer DECIMAL(10,2),
        FOREIGN KEY (category_id) REFERENCES categories(category_id),
        FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
        FOREIGN KEY (material_id) REFERENCES materials(material_id),
        FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
        FOREIGN KEY (collection_id) REFERENCES collections(collection_id)
    )""")
    cur.execute("""CREATE TABLE colors (color_id INTEGER PRIMARY KEY AUTOINCREMENT, color_name TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE sizes (size_id INTEGER PRIMARY KEY AUTOINCREMENT, size_name TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE product_variants (
        variant_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        color_id INTEGER,
        size_id INTEGER,
        stock_quantity INTEGER,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (color_id) REFERENCES colors(color_id),
        FOREIGN KEY (size_id) REFERENCES sizes(size_id)
    )""")
    cur.execute("""CREATE TABLE product_styles (
        product_id INTEGER,
        style_id INTEGER,
        PRIMARY KEY (product_id, style_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (style_id) REFERENCES styles(style_id)
    )""")
    cur.execute("""CREATE TABLE sales (
        sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        sale_date TEXT,
        product_id INTEGER,
        variant_id INTEGER,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        total_price DECIMAL(10,2),
        is_weekend INTEGER,
        is_holiday INTEGER,
        holiday_name TEXT
    )""")
    cur.execute("""CREATE TABLE holidays (
        holiday_date TEXT PRIMARY KEY,
        holiday_name TEXT
    )""")
    conn.commit()
    print("Schema created.")

# ---------------------------
# Master data insertion
# ---------------------------
def seed_master_data(conn):
    cur = conn.cursor()

    # Categories
    categories = [
        "Shirts","T-Shirts","Jeans","Trousers","Sarees","Dresses","Sweaters",
        "Kurtas","Ethnic Wear","Shorts","Skirts","Jackets","Coats","Footwear",
        "Accessories","Kids Wear","Innerwear","Activewear","Formal Wear","Blazers"
    ]

    # Category → Brands mapping
    category_brand_map = {
        "Shirts": ["Allen Solly","Van Heusen","Peter England","Raymond","Louis Philippe"],
        "T-Shirts": ["Nike","Puma","H&M","Zara","United Colors of Benetton","Jack & Jones"],
        "Jeans": ["Levi's","Wrangler","Lee","Pepe Jeans","Spykar"],
        "Trousers": ["Arrow","Van Heusen","Louis Philippe","Allen Solly","H&M"],
        "Sarees": ["Nalli","Biba","FabIndia","Sabyasachi","Manyavar"],
        "Dresses": ["H&M","Zara","Forever 21","FabIndia","Biba"],
        "Sweaters": ["UCB","Puma","Adidas","Peter England","H&M"],
        "Kurtas": ["Biba","FabIndia","Manyavar","W","Aurelia"],
        "Ethnic Wear": ["FabIndia","Biba","Manyavar","Sabyasachi","W"],
        "Shorts": ["Nike","Adidas","Puma","Reebok","H&M"],
        "Skirts": ["H&M","Zara","Forever 21","FabIndia","Biba"],
        "Jackets": ["Levi's","Nike","Puma","Zara","Wildcraft"],
        "Coats": ["Zara","H&M","United Colors of Benetton","Tommy Hilfiger","Mango"],
        "Footwear": ["Bata","Nike","Adidas","Puma","Woodland","Red Tape"],
        "Accessories": ["Fossil","Titan","Hidesign","Fastrack","Aldo"],
        "Kids Wear": ["Gini & Jony","H&M Kids","Mothercare","Carter's","Chicco"],
        "Innerwear": ["Jockey","Enamor","Calvin Klein","Hanes","Amante"],
        "Activewear": ["Nike","Adidas","Puma","Reebok","HRX"],
        "Formal Wear": ["Raymond","Van Heusen","Allen Solly","Louis Philippe","Arrow"],
        "Blazers": ["Van Heusen","Louis Philippe","Allen Solly","Raymond","Zara"]
    }

    # Insert categories
    cur.executemany("INSERT INTO categories (category_name) VALUES (?)", [(c,) for c in categories])
    # Insert unique brands
    brands = sorted(set(b for blist in category_brand_map.values() for b in blist))
    cur.executemany("INSERT INTO brands (brand_name) VALUES (?)", [(b,) for b in brands])

    materials = ["Cotton","Polyester","Silk","Wool","Denim","Linen","Chiffon","Georgette",
                 "Rayon","Velvet","Leather","Suede","Nylon","Viscose"]
    
    # Updated suppliers list with brand_name and contact_info
    suppliers = [
        ("Eastern Fabrics Pvt Ltd", "Allen Solly", "eastern@fabrics.example"),
        ("South Textiles Co", "Biba", "south@textile.example"),
        ("Urban Suppliers", "H&M", "urban@suppliers.example"),
        ("Heritage Mills", "Raymond", "heritage@mills.example"),
        ("Global Fabrics", "Nike", "global@fabrics.example"),
        ("Style Hub", "FabIndia", "stylehub@shop.example")
    ]
    
    collections = ["Summer","Winter","Festive","Casual","Formal","Spring","Autumn","Monsoon"]
    styles = ["Casual","Formal","Ethnic","Sporty","Partywear","Workwear","Lounge"]
    colors = ["Black","White","Red","Blue","Green","Yellow","Pink","Grey","Beige","Brown",
              "Maroon","Navy","Olive","Orange","Purple","Teal","Mustard"]
    sizes = ["XS","S","M","L","XL","XXL","28","30","32","34","36"]

    cur.executemany("INSERT INTO materials (material_name) VALUES (?)", [(m,) for m in materials])
    cur.executemany(
        "INSERT INTO suppliers (supplier_name, brand_name, contact_info) VALUES (?, ?, ?)",
        suppliers
    )
    cur.executemany("INSERT INTO collections (collection_name) VALUES (?)", [(c,) for c in collections])
    cur.executemany("INSERT INTO styles (style_name) VALUES (?)", [(s,) for s in styles])
    cur.executemany("INSERT INTO colors (color_name) VALUES (?)", [(c,) for c in colors])
    cur.executemany("INSERT INTO sizes (size_name) VALUES (?)", [(s,) for s in sizes])
    conn.commit()

    print("Master data seeded.")
    return category_brand_map

# ---------------------------
# Products & variants
# ---------------------------
def seed_products_and_variants(conn, num_products=NUM_PRODUCTS, category_brand_map=None):
    cur = conn.cursor()

    # Fetch IDs
    cat_ids = {name: cid for cid,name in cur.execute("SELECT category_id,category_name FROM categories")}
    brand_ids = {name: bid for bid,name in cur.execute("SELECT brand_id,brand_name FROM brands")}
    mat_ids = [r[0] for r in cur.execute("SELECT material_id FROM materials")]
    sup_ids = [r[0] for r in cur.execute("SELECT supplier_id FROM suppliers")]
    coll_ids = [r[0] for r in cur.execute("SELECT collection_id FROM collections")]
    color_ids = [r[0] for r in cur.execute("SELECT color_id FROM colors")]
    size_ids = [r[0] for r in cur.execute("SELECT size_id FROM sizes")]
    style_ids = [r[0] for r in cur.execute("SELECT style_id FROM styles")]

    def random_price_for_category(cat_name):
        if any(k in cat_name.lower() for k in ["saree","dress","ethnic","coat","jacket","blazer"]):
            base = random.randint(1200,7000)
        elif any(k in cat_name.lower() for k in ["jean","trouser","footwear","formal","blazer"]):
            base = random.randint(900,4000)
        elif any(k in cat_name.lower() for k in ["t-shirt","shirt","shorts","innerwear","activewear"]):
            base = random.randint(300,2500)
        else:
            base = random.randint(400,3500)
        offer = random.choice([0,5,10,15,20,25])
        after = round(base * (1 - offer/100),2)
        return base, offer, after

    # Insert products
    for i in range(1, num_products+1):
        cat_name = random.choice(list(category_brand_map.keys()))
        cat_id = cat_ids[cat_name]
        brand_name = random.choice(category_brand_map[cat_name])
        brand_id = brand_ids[brand_name]
        material_id = random.choice(mat_ids)
        supplier_id = random.choice(sup_ids)
        coll_id = random.choice(coll_ids)
        base_price, offer, price_after = random_price_for_category(cat_name)
        name = f"{brand_name} {cat_name} Style {i}"

        cur.execute("""INSERT INTO products
            (product_name, category_id, brand_id, material_id, supplier_id, collection_id,
             brought_unit_price, current_price, offer, price_after_offer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, cat_id, brand_id, material_id, supplier_id, coll_id,
             base_price, base_price, offer, price_after))

    conn.commit()
    print(f"Inserted {num_products} products.")

    # Variants
    product_ids = [r[0] for r in cur.execute("SELECT product_id FROM products")]
    variant_insert_sql = "INSERT INTO product_variants (product_id, color_id, size_id, stock_quantity) VALUES (?, ?, ?, ?)"
    for pid in product_ids:
        colors_for_product = random.sample(color_ids, k=random.randint(1, min(5,len(color_ids))))
        sizes_for_product = random.sample(size_ids, k=random.randint(1, min(6,len(size_ids))))
        combos = [(pid, c, s, random.randint(10,300)) for c in colors_for_product for s in sizes_for_product]
        if combos:
            chosen = random.sample(combos, k=random.randint(1, min(10,len(combos))))
            cur.executemany(variant_insert_sql, chosen)

    conn.commit()
    print("Product variants inserted.")

    # Styles
    ps_rows = []
    for pid in product_ids:
        chosen = random.sample(style_ids, k=random.randint(1, min(2,len(style_ids))))
        for sid in chosen:
            ps_rows.append((pid,sid))
    cur.executemany("INSERT INTO product_styles (product_id, style_id) VALUES (?, ?)", ps_rows)
    conn.commit()
    print("Product styles linked.")

# ---------------------------
# Sales generation
# ---------------------------
def generate_sales(conn, holidays):
    cur = conn.cursor()
    products = cur.execute("SELECT product_id, product_name, category_id, price_after_offer, current_price FROM products").fetchall()
    if not products:
        raise RuntimeError("No products found.")
    prod_map = {p[0]: {'name':p[1],'category_id':p[2],'price':(p[3] or p[4] or 0)} for p in products}
    variants = cur.execute("SELECT variant_id, product_id FROM product_variants").fetchall()
    variants_by_product = defaultdict(list)
    for v in variants:
        variants_by_product[v[1]].append(v[0])
    product_ids_sorted = sorted(prod_map.keys())
    weights = [(pid, 1.0/(i+1)**0.9) for i,pid in enumerate(product_ids_sorted)]
    s = sum(w for _,w in weights)
    popularity_weights = [(pid,w/s) for pid,w in weights]

    def sample_product_id(pop):
        r=random.random(); cum=0
        for pid,w in pop:
            cum+=w
            if r<=cum: return pid
        return pop[-1][0]

    insert_sql = """INSERT INTO sales (transaction_id,sale_date,product_id,variant_id,quantity,unit_price,total_price,is_weekend,is_holiday,holiday_name)
                    VALUES (?,?,?,?,?,?,?,?,?,?)"""

    current_date=START_DATE; txn_counter=0; rows_inserted=0; commit_counter=0
    while current_date<=END_DATE:
        wd=current_date.weekday(); is_weekend=1 if wd>=5 else 0
        date_str=current_date.strftime("%Y-%m-%d")
        holiday_name=holidays.get(date_str); is_holiday=1 if holiday_name else 0
        mult=1.0
        if is_weekend: mult*=WEEKEND_MULTIPLIER
        if is_holiday: mult*=FESTIVAL_MULTIPLIER
        if current_date.month in (10,11,12): mult*=1.2   # Seasonal spike
        expected=AVG_TRANSACTIONS_PER_DAY*mult
        day_txn_count=max(1,int(random.gauss(expected,expected*0.16)))

        for _ in range(day_txn_count):
            txn_counter+=1; txn_id=f"TX{date_str.replace('-','')}-{txn_counter}"
            pid=sample_product_id(popularity_weights)
            vars=variants_by_product.get(pid)
            if not vars: continue
            var=random.choice(vars)
            qty=random.choices([1,2,3],weights=[0.86,0.12,0.02])[0]
            unit_price=float(prod_map[pid]['price'] or 0)
            total_price=round(unit_price*qty,2)
            cur.execute(insert_sql,(txn_id,date_str,pid,var,qty,unit_price,total_price,is_weekend,is_holiday,holiday_name))
            rows_inserted+=1; commit_counter+=1
            if commit_counter>=BATCH_COMMIT:
                conn.commit(); commit_counter=0
                print(f"Inserted {rows_inserted} sales rows so far ({date_str})")
        current_date+=timedelta(days=1)
    conn.commit(); print(f"Sales generation complete. Rows inserted: {rows_inserted}")

# ---------------------------
# Store holidays
# ---------------------------
def store_holidays_table(conn,holidays):
    if not holidays: return
    cur=conn.cursor()
    rows=[(d,n) for d,n in sorted(holidays.items())]
    cur.executemany("INSERT OR REPLACE INTO holidays (holiday_date,holiday_name) VALUES (?,?)",rows)
    conn.commit(); print(f"Stored {len(rows)} holidays.")

# ---------------------------
# MAIN
# ---------------------------
def main():
    conn = sqlite3.connect(DB)
    create_schema(conn)
    category_brand_map = seed_master_data(conn)
    seed_products_and_variants(conn, NUM_PRODUCTS, category_brand_map)

    years = list(range(START_DATE.year, END_DATE.year+1))
    holidays = fetch_holidays_calendarific(years, "IN")

    store_holidays_table(conn, holidays)
    generate_sales(conn, holidays)
    conn.close()
    print("✅ Database ready:", DB)

if __name__ == "__main__":
    main()
