-- Seed a small, fixed set of warehouses. Everything else (customers,
-- products, orders, ...) is loaded in bulk by scripts/seed_database.py
-- since it needs to happen at COPY speed for 1M+ row volumes.

INSERT INTO commerce.warehouses (warehouse_name, city) VALUES
    ('Mumbai Central',    'Mumbai'),
    ('Delhi North',       'Delhi'),
    ('Bangalore Tech Hub','Bangalore'),
    ('Chennai Port',      'Chennai'),
    ('Hyderabad East',    'Hyderabad'),
    ('Pune West',         'Pune'),
    ('Kolkata Riverside',  'Kolkata'),
    ('Ahmedabad Industrial','Ahmedabad'),
    ('Jaipur Depot',      'Jaipur'),
    ('Lucknow Hub',       'Lucknow'),
    ('Chandigarh North',  'Chandigarh'),
    ('Kochi Coastal',     'Kochi'),
    ('Nagpur Central',    'Nagpur'),
    ('Indore Depot',      'Indore'),
    ('Surat Textile Hub', 'Surat');
