CREATE TABLE IF NOT EXISTS tbl_sales_stream_company (
    -- Identity & Keys
    sale_id             BIGSERIAL PRIMARY KEY,
    transaction_uuid    UUID NOT NULL DEFAULT gen_random_uuid(),
    
    -- Text & Categorical
    customer_name       VARCHAR(100) NOT NULL,
    sales_channel       VARCHAR(50) DEFAULT 'Direct',
    region_code         CHAR(3),
    
    -- Temporal
    transaction_date    DATE NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Boolean & Status
    is_vip_customer     BOOLEAN DEFAULT FALSE,
    is_refunded         BOOLEAN,
    
    -- Numeric Variations
    quantity            INTEGER NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(12, 2) NOT NULL,
    discount_pct        NUMERIC(5, 4),
    tax_amount          DOUBLE PRECISION,
    shipping_weight_kg  REAL,
    exchange_rate       NUMERIC(18, 8),
    
    -- Advanced Types
    tags                TEXT[],
    metadata            JSONB,
    
    -- GEOSPATIAL (New Columns)
    -- Geography: Good for calculating distances in meters/km over the earth's curve
    delivery_location   GEOGRAPHY(POINT, 4326), 
    
    -- Geometry: Good for defining shapes/zones (SRID 4326 = WGS84 standard)
    sales_coverage_area GEOMETRY(POLYGON, 4326) ,
    company_id 			BIGINT
);

INSERT INTO tbl_sales_stream_company  (
    customer_name, 
    sales_channel, 
    region_code, 
    transaction_date, 
    is_vip_customer, 
    is_refunded, 
    quantity, 
    unit_price, 
    discount_pct, 
    tax_amount, 
    shipping_weight_kg, 
    exchange_rate, 
    tags, 
    metadata,
    delivery_location,
    sales_coverage_area,
    company_id
)
WITH generator AS (
    SELECT generate_series(1, 5) AS id
)
SELECT
    -- ... Standard fields ...
    'Customer-' || (floor(random() * 10000)::text) AS customer_name,
    (ARRAY['Online', 'Retail', 'B2B', 'Affiliate'])[floor(random() * 4 + 1)] AS sales_channel,
    (ARRAY['DJR', 'SCM'])[floor(random() * 4 + 1)] AS region_code,
    CURRENT_DATE - (floor(random() * 365) || ' days')::interval AS transaction_date,
    (random() > 0.8) AS is_vip_customer,
    CASE 
        WHEN random() < 0.1 THEN NULL 
        WHEN random() < 0.2 THEN TRUE 
        ELSE FALSE 
    END AS is_refunded,
    floor(random() * 50 + 1)::int AS quantity,
    ROUND((random() * 10000 + 10)::numeric, 2) AS unit_price,
    ROUND((random() * 0.5)::numeric, 4) AS discount_pct,
    (random() * 100) AS tax_amount,
    (random() * 20)::real AS shipping_weight_kg,
    ROUND((random() * 15000 + 14000)::numeric, 8) AS exchange_rate,
    CASE 
        WHEN random() > 0.5 THEN ARRAY['flash_sale', 'electronic']
        ELSE ARRAY['regular', 'household']
    END AS tags,
    jsonb_build_object(
        'device', (ARRAY['mobile', 'desktop', 'tablet'])[floor(random() * 3 + 1)],
        'os_version', floor(random() * 10 + 10),
        'click_id', md5(random()::text)
    ) AS metadata,

    -- ... NEW GEOSPATIAL DATA GENERATION ...

    -- 1. Generate Random Delivery Location (Geography Point)
    -- Logic: ST_SetSRID(ST_MakePoint(Longitude, Latitude), 4326)
    ST_SetSRID(
        ST_MakePoint(
            (random() * 360 - 180)::numeric, -- Longitude: -180 to 180
            (random() * 180 - 90)::numeric   -- Latitude: -90 to 90
        ), 
        4326
    )::geography AS delivery_location,

    -- 2. Generate Random Coverage Area (Geometry Polygon)
    -- Logic: Create a point, then 'buffer' it by 0.1 degrees (~11km) to make a circle polygon
    ST_Buffer(
        ST_SetSRID(
            ST_MakePoint(
                (random() * 360 - 180)::numeric, 
                (random() * 180 - 90)::numeric
            ), 
            4326
        ), 
        0.1 -- Buffer size in degrees (creates a small circle around the point)
    ) AS sales_coverage_area,
    1

FROM generator;