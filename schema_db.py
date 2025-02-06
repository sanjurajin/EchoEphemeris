import os
import psycopg2
_6f2f7e6c = os.getcwd()
if os.getlogin() == 'sanju':
    var_33b379a7 = os.path.join(_6f2f7e6c, 'personal_data', 'creden.txt')
    sa_631aada4 = open(var_33b379a7, 'r').read().split()
    var_b4c16391 = sa_631aada4[0]
    sa_a0e71f8f = sa_631aada4[1]
    var_2e48e91c = sa_631aada4[2]
else:
    var_33b379a7 = os.path.join(_6f2f7e6c, 'app', 'creden.txt')
    sa_631aada4 = open(var_33b379a7, 'r').read().split()
    var_b4c16391 = sa_631aada4[0]
    sa_a0e71f8f = sa_631aada4[1]
    var_2e48e91c = sa_631aada4[2]

def zx_4414491f(var_2e48e91c, var_b4c16391):
    conn = psycopg2.connect(database='postgres', user='postgres', password=var_2e48e91c, host='localhost', port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(f'CREATE DATABASE {var_b4c16391};')
        print("Database 'my_delivery_data' created successfully.")
        sa_073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        sa_073c1634 = False
    finally:
        cursor.close()
        conn.close()
    return sa_073c1634

def xea13d6bb(var_b4c16391, var_2e48e91c):
    conn = psycopg2.connect(database=var_b4c16391, user='postgres', password=var_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute('\n                      CREATE TABLE my_static_data (\n                            static_id SERIAL PRIMARY KEY,\n                            isin VARCHAR(20) NOT NULL UNIQUE,\n                            fininstrmid VARCHAR(10),\n                            fininstrmid_b VARCHAR(10),\n                            tckrsymb VARCHAR(20),\n                            tckrsymb_b VARCHAR(20),\n                            src VARCHAR(10),\n                            src_b VARCHAR(10),\n                            fininstrmnm VARCHAR(50) );\n\n                    ')
        print("Table 'my_static_data' created successfully.")
        sa_073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        sa_073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return sa_073c1634
 
def x47b43f5a(var_b4c16391, var_2e48e91c):
    conn = psycopg2.connect(database=var_b4c16391, user='postgres', password=var_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute('\n                       CREATE TABLE my_daily_data (\n                            ISIN VARCHAR(20) NOT NULL,\n                            TradDt DATE NOT NULL,\n                            sctysrs VARCHAR(10),\n                            sctysrs_b VARCHAR(10),\n                            opnpric DECIMAL(12,2),\n                            hghpric DECIMAL(12,2),\n                            lwpric DECIMAL(12,2),\n                            clspric DECIMAL(12,2),\n                            lastpric DECIMAL(12,2),\n                            prvsclsgpric DECIMAL(12,2),\n                            sttlmpric DECIMAL(12,2),\n                            ttltradgvol BIGINT,\n                            ttltrfval BIGINT,\n                            ttlnboftxsexctd BIGINT,\n                            qty_del BIGINT,\n                            delvry_trnovr BIGINT,\n                            ttltradgvol_b BIGINT,\n                            ttltrfval_b BIGINT,\n                            ttlnboftxsexctd_b BIGINT,\n                            qty_del_b BIGINT,\n                            delvry_trnovr_b BIGINT,\n                            sum_ttltradgvol BIGINT,\n                            sum_ttltrfval BIGINT,\n                            sum_ttlnboftxsexctd BIGINT,\n                            sum_del_qty BIGINT,\n                            sum_delvry_trnovr BIGINT,\n                            del_per DECIMAL(12,2),\n                            avg_price DECIMAL(12,2),\n                            avg_qty_per_order BIGINT,\n                            avg_order_price DECIMAL(12,2),\n                            close_to_avg DECIMAL(12,2),\n                            close_avg_perc DECIMAL(12,2),\n                            othr_trds VARCHAR(20),\n                            othr_trds_vol BIGINT,\n                            othr_trds_val BIGINT,\n                            othr_trds_txsexctd BIGINT,\n                            events TEXT,\n                            FOREIGN KEY (ISIN) REFERENCES my_static_data(ISIN) ON DELETE CASCADE,\n                            CONSTRAINT my_daily_data_pkey PRIMARY KEY (ISIN, TradDt, sctysrs, sctysrs_b)\n                                )\n                                PARTITION BY RANGE (TradDt);\n                        ')
        print("Table 'my_daily_data' created successfully.")
        sa_073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        sa_073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return sa_073c1634

def xc4dfeba4(var_b4c16391, var_2e48e91c):
    conn = psycopg2.connect(database=var_b4c16391, user='postgres', password=var_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute("\n                        CREATE TABLE my_daily_data_q1_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');\n                        CREATE TABLE my_daily_data_q2_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');\n                        CREATE TABLE my_daily_data_q3_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');\n                        CREATE TABLE my_daily_data_q4_2024 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');\n                        CREATE TABLE my_daily_data_q1_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');\n                        CREATE TABLE my_daily_data_q2_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');\n                        CREATE TABLE my_daily_data_q3_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');\n                        CREATE TABLE my_daily_data_q4_2025 PARTITION OF my_daily_data\n                            FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');\n                       \n                        ")
        print("Partitions for 'my_daily_data' created successfully.")
        sa_073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        sa_073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return sa_073c1634

def sa_49d6f108(var_b4c16391, var_2e48e91c):
    conn = psycopg2.connect(database=var_b4c16391, user='postgres', password=var_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute('\n                        CREATE TABLE isin_mapping (\n                            mapping_id SERIAL PRIMARY KEY,  -- Unique identifier for each mapping\n                            symbol VARCHAR(20),\n                            isin character varying(12) NOT NULL,\n                            ref_isin character varying(12) NOT NULL,\n                            FOREIGN KEY (isin) REFERENCES my_static_data(isin),  -- Ensure isin exists in static_data\n                            FOREIGN KEY (ref_isin) REFERENCES my_static_data(isin),  -- Ensure ref_isin exists in static_data\n                            UNIQUE (isin, ref_isin)  -- Ensure that the combination of isin and ref_isin is unique\n                        );\n                        ')
        print("Table  'isin_mapping' created successfully.")
        sa_073c1634 = True
    except Exception as e:
        print(f'An error occurred: {e}')
        sa_073c1634 = False
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return sa_073c1634

def xe46c5d58(var_b4c16391, sa_a0e71f8f, var_2e48e91c):
    conn = psycopg2.connect(database=var_b4c16391, user=sa_a0e71f8f, password=var_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        cursor.execute("\n            SELECT table_name \n            FROM information_schema.tables \n            WHERE table_schema = 'public';  -- Change schema if needed\n        ")
        tables = cursor.fetchall()
        print('Tables in the database:')
        for table in tables:
            print(table[0])
    except Exception as e:
        print(f'An error occurred: {e} while listing Tables')
        sa_073c1634 = False
    finally:
        cursor.close()
        conn.close()

def sa_cf342737(var_b4c16391, var_2e48e91c):
    from psycopg2 import sql
    conn = psycopg2.connect(database='postgres', user='postgres', password=var_2e48e91c, host='localhost', port='5432')
    cursor = conn.cursor()
    try:
        query = sql.SQL('SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;')
        cursor.execute(query, (var_b4c16391,))
        exists = cursor.fetchone() is not None
    except Exception as e:
        print(f'An error occurred: {e}')
        sa_073c1634 = False
    finally:
        cursor.close()
        conn.close()
    if exists:
        print(f"The database '{var_b4c16391}' already exists.")
        sa_073c1634 = False
    else:
        print(f"The database '{var_b4c16391}' does not exist.")
        sa_073c1634 = True
    cursor.close()
    conn.close()
    return sa_073c1634
sa_073c1634 = sa_cf342737(var_b4c16391, var_2e48e91c)
if sa_073c1634:
    sa_073c1634 =zx_4414491f(var_2e48e91c, var_b4c16391)
if sa_073c1634:
    sa_073c1634 = xea13d6bb(var_b4c16391,var_2e48e91c)
if sa_073c1634:
    sa_073c1634 = x47b43f5a(var_b4c16391,var_2e48e91c)
if sa_073c1634:
    sa_073c1634 = xc4dfeba4(var_b4c16391, var_2e48e91c)
if sa_073c1634:
    sa_073c1634 = sa_49d6f108(var_b4c16391, var_2e48e91c)
if sa_073c1634:
    print('Database setup successfully done')
    xe46c5d58(var_b4c16391, sa_a0e71f8f, var_2e48e91c)
if not sa_073c1634:
    xe46c5d58(var_b4c16391, sa_a0e71f8f, var_2e48e91c)