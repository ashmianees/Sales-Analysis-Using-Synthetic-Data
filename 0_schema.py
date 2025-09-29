import sqlite3

# Connect to SQLite database (creates if not exists)
conn = sqlite3.connect("textiles.db")
cursor = conn.cursor()

# --- Drop tables if exist (for clean run) ---
tables = [
    "product_styles", "product_variants", "products",
    "categories", "brands", "materials", "suppliers",
    "collections", "styles", "colors", "sizes"
]
for t in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {t}")

# --- Create Tables ---

cursor.execute("""
CREATE TABLE categories (
    category_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE brands (
    brand_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_name TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE materials (
    material_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    material_name TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE suppliers (
    supplier_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name  TEXT NOT NULL,
    contact_info   TEXT
)
""")

cursor.execute("""
CREATE TABLE collections (
    collection_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_name TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE styles (
    style_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    style_name TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE products (
    product_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name        TEXT NOT NULL,
    category_id         INTEGER,
    brand_id            INTEGER,
    material_id         INTEGER,
    supplier_id         INTEGER,
    collection_id       INTEGER,
    brought_unit_price  DECIMAL(10,2),
    current_price       DECIMAL(10,2),
    offer               DECIMAL(5,2),
    price_after_offer   DECIMAL(10,2),
    FOREIGN KEY (category_id) REFERENCES categories(category_id),
    FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
    FOREIGN KEY (material_id) REFERENCES materials(material_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id),
    FOREIGN KEY (collection_id) REFERENCES collections(collection_id)
)
""")

cursor.execute("""
CREATE TABLE colors (
    color_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    color_name  TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE sizes (
    size_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    size_name  TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE product_variants (
    variant_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id      INTEGER,
    color_id        INTEGER,
    size_id         INTEGER,
    stock_quantity  INTEGER,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (color_id) REFERENCES colors(color_id),
    FOREIGN KEY (size_id) REFERENCES sizes(size_id)
)
""")

cursor.execute("""
CREATE TABLE product_styles (
    product_id   INTEGER,
    style_id     INTEGER,
    PRIMARY KEY (product_id, style_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (style_id) REFERENCES styles(style_id)
)
""")

# Commit and close
conn.commit()
conn.close()

print("✅ Database and tables created successfully!")
