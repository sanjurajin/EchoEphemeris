import os
import psycopg2
cwd = os.getcwd()
x33b379a7 = os.path.join(cwd, 'app', 'creden.txt')
sA_631aada4 = open(x33b379a7, 'r').read().split()
xb4c16391 = sA_631aada4[0]
xa0e71f8f = sA_631aada4[1]
sA_2e48e91c = sA_631aada4[2]
def zx_4414491f(sA_2e48e91c):
    conn = psycopg2.connect(database='postgres', user='postgres', password=sA_2e48e91c, host='localhost', port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute('CREATE DATABASE EchoEphemeris;')
        print("Database 'my_delivery_data' created successfully.")
        x073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        x073c1634 = False
    finally:
        cursor.close()
        conn.close()
    return x073c1634
def _ea13d6bb(xb4c16391, sA_2e48e91c):
    conn = psycopg2.connect(database=xb4c16391, user='postgres', password=sA_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute('\n                      CREATE TABLE my_static_data (\n                            static_id SERIAL PRIMARY KEY,\n                            isin VARCHAR(20) NOT NULL UNIQUE,\n                            fininstrmid VARCHAR(10),\n                            fininstrmid_b VARCHAR(10),\n                            tckrsymb VARCHAR(20),\n                            tckrsymb_b VARCHAR(20),\n                            src VARCHAR(10),\n                            src_b VARCHAR(10),\n                            fininstrmnm VARCHAR(50) );\n\n                    ')
        print("Table 'my_static_data' created successfully.")
        x073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        x073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return x073c1634
def x47b43f5a(xb4c16391, sA_2e48e91c):
    conn = psycopg2.connect(database=xb4c16391, user='postgres', password=sA_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute('\n                       CREATE TABLE my_daily_data (\n                            ISIN VARCHAR(20) NOT NULL,\n                            TradDt DATE NOT NULL,\n                            sctysrs VARCHAR(10),\n                            sctysrs_b VARCHAR(10),\n                            opnpric DECIMAL(12,2),\n                            hghpric DECIMAL(12,2),\n                            lwpric DECIMAL(12,2),\n                            clspric DECIMAL(12,2),\n                            lastpric DECIMAL(12,2),\n                            prvsclsgpric DECIMAL(12,2),\n                            sttlmpric DECIMAL(12,2),\n                            ttltradgvol BIGINT,\n                            ttltrfval BIGINT,\n                            ttlnboftxsexctd BIGINT,\n                            qty_del BIGINT,\n                            delvry_trnovr BIGINT,\n                            ttltradgvol_b BIGINT,\n                            ttltrfval_b BIGINT,\n                            ttlnboftxsexctd_b BIGINT,\n                            qty_del_b BIGINT,\n                            delvry_trnovr_b BIGINT,\n                            sum_ttltradgvol BIGINT,\n                            sum_ttltrfval BIGINT,\n                            sum_ttlnboftxsexctd BIGINT,\n                            sum_del_qty BIGINT,\n                            sum_delvry_trnovr BIGINT,\n                            del_per DECIMAL(12,2),\n                            avg_price DECIMAL(12,2),\n                            avg_qty_per_order BIGINT,\n                            avg_order_price DECIMAL(12,2),\n                            close_to_avg DECIMAL(12,2),\n                            close_avg_perc DECIMAL(12,2),\n                            othr_trds VARCHAR(20),\n                            othr_trds_vol BIGINT,\n                            othr_trds_val BIGINT,\n                            othr_trds_txsexctd BIGINT,\n                            events TEXT,\n                            FOREIGN KEY (ISIN) REFERENCES my_static_data(ISIN) ON DELETE CASCADE,\n                            CONSTRAINT my_daily_data_pkey PRIMARY KEY (ISIN, TradDt, sctysrs, sctysrs_b)\n                                )\n                                PARTITION BY RANGE (TradDt);\n                        ')
        print("Table 'my_daily_data' created successfully.")
        x073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        x073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return x073c1634

def sA_c4dfeba4(xb4c16391, sA_2e48e91c):
    conn = psycopg2.connect(database=xb4c16391, user='postgres', password=sA_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute("\n                        CREATE TABLE my_daily_data_q1_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');\n                        CREATE TABLE my_daily_data_q2_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');\n                        CREATE TABLE my_daily_data_q3_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');\n                        CREATE TABLE my_daily_data_q4_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');\n                        CREATE TABLE my_daily_data_q1_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');\n                        CREATE TABLE my_daily_data_q2_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');\n                        CREATE TABLE my_daily_data_q3_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');\n                        CREATE TABLE my_daily_data_q4_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');\n                       \n                        ")
        print("Partitions for 'my_daily_data' created successfully.")
        x073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        x073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return x073c1634

def var_49d6f108(xb4c16391, sA_2e48e91c):
    conn = psycopg2.connect(database=xb4c16391, user='postgres', password=sA_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute('\n                        CREATE TABLE isin_mapping (\n                            mapping_id SERIAL PRIMARY KEY,  -- Unique identifier for each mapping\n                            symbol VARCHAR(20),\n                            isin character varying(12) NOT NULL,\n                            ref_isin character varying(12) NOT NULL,\n                            FOREIGN KEY (isin) REFERENCES my_static_data(isin),  -- Ensure isin exists in static_data\n                            FOREIGN KEY (ref_isin) REFERENCES my_static_data(isin),  -- Ensure ref_isin exists in static_data\n                            UNIQUE (isin, ref_isin)  -- Ensure that the combination of isin and ref_isin is unique\n                        );\n                        ')
        print("Table  'isin_mapping' created successfully.")
        x073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        x073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return x073c1634

def xe46c5d58(xb4c16391, xa0e71f8f, sA_2e48e91c):
    conn = psycopg2.connect(database=xb4c16391, user=xa0e71f8f, password=sA_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute("\n            SELECT table_name \n            FROM information_schema.tables \n            WHERE table_schema = 'public';  -- Change schema if needed\n        ")
        sA_d98a034f = cursor.fetchall()
        print('Tables in the database:')
        for x0d4fc4a7 in sA_d98a034f:
            print(x0d4fc4a7[0])
    except Exception as e:
        print(f'An error occurred: {e} while listing Tables')
        x073c1634 = False
    finally:
        cursor.close()
        conn.close()

def _cf342737(xb4c16391, sA_2e48e91c):
    from psycopg2 import sql
    conn = psycopg2.connect(database='postgres', user='postgres', password=sA_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        query = sql.SQL('SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;')
        cursor.execute(query, (xb4c16391,))
        exists = cursor.fetchone() is not None
    except Exception as e:
        print(f'An error occurred: {e}')
        x073c1634 = False
    finally:
        cursor.close()
        conn.close()
    if exists:
        print(f"The database '{xb4c16391}' already exists.")
        x073c1634 = False
    else:
        print(f"The database '{xb4c16391}' does not exist.")
        x073c1634 = True
    cursor.close()
    conn.close()
    return x073c1634
x073c1634 = _cf342737(xb4c16391, sA_2e48e91c)
if x073c1634:
    x073c1634 = zx_4414491f(db_password=sA_2e48e91c)
if x073c1634:
    x073c1634 = _ea13d6bb(db_name=xb4c16391, db_password=sA_2e48e91c)
if x073c1634:
    x073c1634 = x47b43f5a(db_name=xb4c16391, db_password=sA_2e48e91c)
if x073c1634:
    x073c1634 = sA_c4dfeba4(db_name=xb4c16391, db_password=sA_2e48e91c)
if x073c1634:
    x073c1634 = var_49d6f108(db_name=xb4c16391, db_password=sA_2e48e91c)
if x073c1634:
    print('Database setup successfully done')
    xe46c5d58(xb4c16391, xa0e71f8f, sA_2e48e91c)
if not x073c1634:
    xe46c5d58(xb4c16391, xa0e71f8f, sA_2e48e91c)