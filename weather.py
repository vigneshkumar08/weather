import requests
import pandas as pd
import psycopg2


def weather(url, date):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        extracted_data = [
            {
                "_id": item["_id"],
                "rainfall": item.get("rainfall", 0),
                "station_name": item["station_name"],
                "latitude": item["latitude"],
                "longitude": item["longitude"],
                "district_name": item["district_name"],
                "taluk_name": item.get("taluk_name", "Unknown"),  
                "firka_name": item["firka_name"],
                "date": date
            }
            for item in data
        ]
        return pd.DataFrame(extracted_data)
    else:
        raise Exception(f"Failed to fetch data. Status Code: {response.status_code}")


def csv(df, file_name):
    df.to_csv(file_name, index=False)
    print(f"Data successfully saved to {file_name}")


def create_table(cursor, df):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather(
        _id VARCHAR(100),
        rainfall FLOAT,
        station_name VARCHAR(100),
        latitude VARCHAR(50),
        longitude VARCHAR(50),
        district_name VARCHAR(50),
        taluk_name VARCHAR(50),
        firka_name VARCHAR(100),
        date VARCHAR(100)
    );
    """
    cursor.execute(create_table_query)

    for _, row in df.iterrows():
        district_name_trimmed = (row['district_name'] or "").strip()
        taluk_name_trimmed = (row['taluk_name'] or "Unknown").strip()  
        station_name_trimmed=(row['station_name']).strip()
        firka_name_trimmed=(row['firka_name']).strip()
        # Fetch district_id
        cursor.execute(
            "SELECT district_id FROM district_id WHERE LOWER(TRIM(district_name)) = LOWER(%s)",
            (district_name_trimmed,)
        )
        district_id_result = cursor.fetchone()
        if district_id_result:
            district_id = district_id_result[0]
        else:
            print(f"District name '{district_name_trimmed}' not found in weat_dist table.")
            continue
        #fetch station_id
        cursor.execute(
            "SELECT station_id FROM station_name WHERE LOWER(TRIM(station_name))=LOWER(%s)",
            (station_name_trimmed,)
        )
        station_id_result=cursor.fetchone()
        if station_id_result:
            station_id=station_id_result[0]
        else:
            print(f"station is not fount '{station_name_trimmed }' not station_name")
            cursor.execute(
                "INSERT INTO station_name (station_name) VALUES (%s) RETURNING station_id;",
                (station_name_trimmed,)
            )
            taluk_id = cursor.fetchone()[0]
            continue
        #fetch firka_name
        cursor.execute(
            "SELECT firka_id FROM firka_name WHERE LOWER(TRIM(firka_name)) = LOWER(%s)",
            (firka_name_trimmed,)
        )
        firka_id_result=cursor.fetchone()
        if firka_id_result:
            firka_id=firka_id_result[0]
        # Fetch  taluk_id
        cursor.execute(
            "SELECT taluk_id FROM taluk_id WHERE LOWER(TRIM(taluk_name)) = LOWER(%s)",
            (taluk_name_trimmed,)
        )
        taluk_id_result = cursor.fetchone()
        if taluk_id_result:
            taluk_id = taluk_id_result[0]
        else:
            print(f"Taluk name '{taluk_name_trimmed}' not found in taluk_id table. Adding it.")
            cursor.execute(
                "INSERT INTO taluk_id (taluk_name) VALUES (%s) RETURNING taluk_id;",
                (taluk_name_trimmed,)
            )
            taluk_id = cursor.fetchone()[0]

        # Insert weather data
        insert_query = """
        INSERT INTO weather (_id, rainfall, station_id, latitude, longitude, district_id, taluk_id, firka_id, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        data_tuple = (
            row["_id"],
            float(row["rainfall"]),
            station_id,
            row["latitude"],
            row["longitude"],
            district_id,
            taluk_id,
            firka_id,
            row["date"]
        )
        cursor.execute(insert_query, data_tuple)


def main():
    url = "https://weather.tnsdma.tn.gov.in/tndrra/FetchData/arg Daily/2024-12-09"
    date = "09-12-2024"
    db_config = {
        "dbname": "postgres",
        "user": "admin",
        "password": "godspeed123",
        "host": "192.168.50.26",
        "port": "5431"
    }

    try:
        df = weather(url, date)
        csv(df, 'weather_data.csv')

        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        create_table(cursor, df)
        connection.commit()
        print("Data has been inserted into PostgreSQL.")

    except Exception as e:
        print("An error occurred:", e)

    finally:
        if 'connection' in locals() and connection:
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()
