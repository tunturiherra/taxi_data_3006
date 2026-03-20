# main.py
import io
import traceback
from traceback import print_exc
import requests
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def download_taxi_data(year, month, taxi_type="yellow"):
    """

    funktio hakee cloudfrontista tiedoston vuoden, kuukauden ja taksin tyypin mukaan

    year: int
    month: int
    taxi_type: str (oletusarvo: yellow)

    New yorkissa on ainakin keltaisia ja vihreitä takseja

    """

    # linkin muoto: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

    # month:02d tekee kuukaudesta varmasti kaksinumeroisen. Jos kuukausi on alle 10, numeroon lisätään etunolla

    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

    print(f"Ladataan tiedostoa: {file_name}...")

    # requests.get tekee HTTP GET-metodilla pyynnön osoitteeseen
    # response sisältää urlissa olevan tiedoston datan
    response = requests.get(url)

    # open-funktio luo ja avaa omalla koneella tiedoston samalla nimellä 'wb'-moodisas
    # w tarkoittaa kirjotusta (eli voimme kirjoittaa tiedostoon netistä haetun datan)
    # b tarkoittaa binarya (.parquet-tiedostoformaatti on binääriä)

    """
    Jos tiedoston nimi on yellow_tripdata_2024-01.parquet
    open luo omalle koneelle tiedoston samalla nimellä
    ja kirjoittaa netistä haetun sisällön siihen

    """

    with open(f"taxi_data/{file_name}", 'wb') as f:
        f.write(response.content)

    # kun tiedosto on haettu netistä
    # funktio palauttaa tiedostopolun, jotta sitä voidaan käyttää toisissa funktioissa
    return f"taxi_data/{file_name}"


def parquet_to_csv(_file):
    """

    funktio ottaa parquet-tiedoston polun parametirina
    avaa tiedoston ja muuttaa parquet-tiedoston csv-tiedostoksi

    miksi? Python ei tarvitse csv-tiedostoa mihinkään,
    mutta koska parquet-tiedosto on binääriä, se ei ole ihmisluettavassa muodossa.
    jos emme näe, mitä tietoja taksimatkoista tallennetaan, emme voi suunnitella tietokantaa

    """

    df = pd.read_parquet(_file)
    csv_file = _file.replace(".parquet", ".csv")

    # \t: tarkoittaa tabia eli sarkainta. Sarkainta käytetään kolumnien erottimena
    # koska taksidatan tekstit sisältävät paljon pilkkuja, niitä ei kannata käyttää erotinmerkkeinä

    # header=True laittaa tiedostoon kolumnien otsikot
    # index=False riville ei tarvitse lisätä erikseen yksilöllistä rivinumeroa, koska tietokanta tekee sen autom.

    df.to_csv(csv_file, sep='\t', header=True, index=False)

def _populate_vendors():
    with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
        with conn.cursor() as cur:

            cur.execute("DELETE FROM vendors")
            conn.commit()

            _query = 'INSERT INTO vendors("VendorID", vendor_name) VALUES (%s, %s)'
            vendors = {1: 'Creative Mobile Technologies (CMT)', 2: 'VeriFone Inc. (VTS)', 3: 'Other/Unknown'}
        # try:
            for key, value in vendors.items():
                cur.execute(_query, (key, value))
            conn.commit()
        # except Exception as e:
            conn.rollback()
            # print_exc()


def _populate_payment_types():
    with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
        with conn.cursor() as cur:

            cur.execute("DELETE FROM payment_types;")
            conn.commit()

            _query = 'INSERT INTO payment_types(id, payment_type) VALUES (%s, %s)'
            data = {1: 'Credit Card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip',
                    0: 'Flex fare'}
            try:
                for key, value in data.items():
                    cur.execute(_query, (key, value))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print_exc()


def _populate_boroughs():
    with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
        with conn.cursor() as cur:

            cur.execute("DELETE FROM zones;")
            cur.execute("DELETE FROM boroughs;")
            conn.commit()

            _query = 'INSERT INTO boroughs(borough_id, borough_name) VALUES (%s, %s)'
            boroughs = {1: 'Manhattan', 2: 'Brooklyn', 3: 'Queens', 4: 'Bronx', 5: 'Staten Island', 6: 'EWR', 7: 'Unknown'}
            try:
                for key, value in boroughs.items():
                    cur.execute(_query, (key, value))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print_exc()

def _populate_service_zones():
    with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
        with conn.cursor() as cur:

            cur.execute("DELETE FROM zones;")
            cur.execute("DELETE FROM service_zones;")
            conn.commit()

            _query = 'INSERT INTO service_zones(id, service_zone_name) VALUES (%s, %s)'
            zones = {1: 'Yellow Zone', 2: 'Boro Zone', 3: 'Airports', 4: 'EWR', 5: 'N/A'}
            try:
                for key, value in zones.items():
                    cur.execute(_query, (key, value))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print_exc()

def _populate_rate_codes():
    with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
        with conn.cursor() as cur:

            cur.execute("DELETE FROM rate_codes;")
            conn.commit()

            _query = 'INSERT INTO rate_codes("RatecodeID", code) VALUES (%s, %s)'
            rate_codes = {1: 'Standard Rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare',
                          6: 'Group ride', 99: 'Unknown/Faulty'}
            try:
                for key, value in rate_codes.items():
                    cur.execute(_query, (key, value))
                conn.commit()
            except Exception as e:
                conn.rollback()
                print_exc()

def _populate_zones():
    with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
        with conn.cursor() as cur:

    # poistetaan ensin mahdolliset vanhat rivit taulusta ennen lisäystä päällekkäisyyksien välttämiseksi

         #   clean_yellow_trips()
            cur.execute("DELETE FROM zones;")
            conn.commit()

            # luetaan csv-tiedoston sisältö pandasilla
            # pandas muuttaa rivit oletuksena, joissa lukee N/A, nulliksi (eli jättää tyhjäksi)
            # keep_default_na jättää N/A arvot ennalleen

            df = pd.read_csv('taxi_zone_lookup.csv', keep_default_na=False)

            # csv-tiedostossa on otsikot LocationID, Borough, Zone ja service_zone
            # mutta tässä muutetaan otsikot vastaamaan zones-taulun sarakkeita
            df.columns = ['LocationID', 'borough_id', 'zone_name', 'service_zone_id']

            # aiemmin boroughs-tauluun lisättyjen alueiden nimet ja id-sarakkeen arvot
            borough_map = {
                'Manhattan': 1, 'Brooklyn': 2, 'Queens': 3, 'Bronx': 4,
                'Staten Island': 5, 'EWR': 6, 'Unknown': 7, 'N/A': 7
            }
            # aiemmin service_zones-tauluun lisättyjen alueiden nimet ja id-sarakkeen arvot
            service_map = {
                'Yellow Zone': 1, 'Boro Zone': 2, 'Airports': 3, 'EWR': 4, 'Unknown': 5, 'N/A': 5
            }

            # alkuperäisessä csv:ssä Borough sarakkeessa lukee Manhattan, mutta
            # se se ei toimi meillä services-taulussa tietokannassa, koska borough_id on tyyppiä int,
            # joka on viiteavainkenttä
            # boroughs-tauluun

            # map(borough_map) muuttaa arvon Manhanttan => 1, Brooklyn = 2, Queens => 3, Bronx => 4, Staten Island => 5, EWR => 6, Unknown => 7, N/A => 7

            # fillna(7) on toinen ketjutettu funktiokutsu
            # jos sarake on csv-tiedostossa tyhjä, fillna täyttää siihen arvon
            # tässä tapausessa siihen tulee 7, mikä on yhtä kuin Unknown / N/A

            # astype(int) on kolmas ketjutettu funktiokutsu
            # astype muuttaa sarakkeen arvon tietotyypin. Tässä tapauksessa siitä tulee kokonaisluku
            df['borough_id'] = df['borough_id'].map(borough_map).fillna(7).astype(int)

            # samat toimenpiteet tehdään service_zone_id-sarakkeelle

            # map vaihtaa tekstimuotoisen service_zone_id:n nimen service_map-muuttujaa vastaavaksi numeroksi
            # fillna laittaa tyhjiin sarakkeisiin 5
            # ja lopuksi tietotyyppi muutetaan integeriksi

            df['service_zone_id'] = df['service_zone_id'].map(service_map).fillna(5).astype(int)

            # kysely, joka suoritetaan jokaisen csv-tiedoston rivin kohdalla
            query = 'INSERT INTO zones("LocationID", zone_name, service_zone_id, borough_id) VALUES (%s, %s, %s, %s);'
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rate_codes;")
            conn.commit()
            try:
                for index, row in df.iterrows():
                    cur.execute(query,
                                (row['LocationID'], row['zone_name'], row['service_zone_id'], row['borough_id']))
                conn.commit()
            except Exception as e:
                conn.rollback()
                traceback.print_exc()

## def clean_yellow_trips():
   ## with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
     ##   with conn.cursor() as cur:
       ##     cur = conn.cursor()
         ##   print(" Truncating trips  ")
            # toimii samoin kuin DELETE FROM yellow_trips;
           # cur.execute("TRUNCATE yellow_trips;")
            # conn.commit()

def process_and_copy_yellow_trips(_file):
    df = pd.read_parquet(_file)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df['pu_year'] = df['tpep_pickup_datetime'].dt.year
    df['pu_month'] = df['tpep_pickup_datetime'].dt.month
    df['pu_day'] = df['tpep_pickup_datetime'].dt.day
    df['pu_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['pu_min'] = df['tpep_pickup_datetime'].dt.minute
    df['pu_sec'] = df['tpep_pickup_datetime'].dt.second

    df['do_year'] = df['tpep_dropoff_datetime'].dt.year
    df['do_month'] = df['tpep_dropoff_datetime'].dt.month
    df['do_day'] = df['tpep_dropoff_datetime'].dt.day
    df['do_hour'] = df['tpep_dropoff_datetime'].dt.hour
    df['do_min'] = df['tpep_dropoff_datetime'].dt.minute
    df['do_sec'] = df['tpep_dropoff_datetime'].dt.second

    df.rename(columns={'airport_fee': 'Airport_fee'}, inplace=True)

    df['passenger_count'] = df['passenger_count'].fillna(0).astype(int)
    df['RatecodeID'] = df['RatecodeID'].fillna(99).astype(int)
    # koska yellow_trips-taulussa payment_type on pakollinen kenttä, joka ei voi olla tyhjä
    # laitetaan tyhjiin sarakkeisiin 5
    df['payment_type'] = df['payment_type'].fillna(5).astype(int)

    # koska yellow_trips-taulussa store_and_fwd_flag on pakollinen kenttä, joka ei voi olla tyhjä
    # laitetaan tyhjiin sarakkeisiin N
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')

    # koska yellow_trips-taulussa congestion_surcharge on pakollinen kenttä, joka ei voi olla tyhjä
    # laitetaan tyhjiin sarakkeisiin 0
    df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)

    # koska yellow_trips-taulussa Airport_fee on pakollinen kenttä, joka ei voi olla tyhjä
    # laitetaan tyhjiin sarakkeisiin 0
    df['Airport_fee'] = df['Airport_fee'].astype(float).fillna(0)

    df['VendorID'] = df['VendorID'].where(df['VendorID'].isin([1, 2]), 3)

    with psycopg2.connect(database=os.getenv("DB"), user=os.getenv("DB_USER"), password=os.getenv("DB_PWD")) as conn:
        with conn.cursor() as cur:
            try:
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)

                cols = (
                    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                    'passenger_count', 'trip_distance', 'RatecodeID',
                    'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
                    'payment_type', 'fare_amount', 'extra', 'mta_tax',
                    'tip_amount', 'tolls_amount', 'improvement_surcharge',
                    'total_amount', 'congestion_surcharge', 'Airport_fee',
                    'pu_year', 'pu_month', 'pu_day', 'pu_hour', 'pu_min', 'pu_sec',
                    'do_year', 'do_month', 'do_day', 'do_hour', 'do_min', 'do_sec'
                )
                cur.copy_from(output, "yellow_trips", sep='\t', null='', columns=cols)
                conn.commit()

            except Exception as e:
                conn.rollback()
                traceback.print_exc()

def run():
    # ikiluuppi pyörii koko ajan, jos ei käyttäjä katkaise suoritusta valitsemalla 0
    while True:
        _choice = input(
            "Valitse, mitä haluat tehdä:  "
            "\n0: lopeta, "
            "\n1: lataa tiedosto netistä ja muuta .csv:ksi"
            "\n2: Vie vendorit tietokantaan"
            "\n3: Vie payment_types tietokantaan"
            "\n4: Vie boroughs tietokantaan"
            "\n5: Vie service_zonet tietokantaan"
            "\n6: Vie rate codet tietokantaan"
            "\n7: Vie zonet tietokantaan"
            "\n8: Vie yhden kuukauden datat tietokantaan\n"
        )

        # jos käyttäjä valitsee 0, ohjelman suoritus lopetetaan
        if _choice == "0":
            break
        # jos valitaan 1
        elif _choice == "1":
            # kysytään vuosi
            _y = input("Anna vuosi:  ")
            # ja kuukausi
            _m = input("Anna kuukausi:  ")
            # ladataan parquet-tiedosto
            file = download_taxi_data(int(_y), int(_m))
            # tallennetaan parquet-tiedosto csv-tiedostona
            parquet_to_csv(file)
        elif _choice == "2":
            _populate_vendors()
        elif _choice == "3":
            _populate_payment_types()
        elif _choice == "4":
            _populate_boroughs()
        elif _choice == "5":
            _populate_service_zones()
        elif _choice == "6":
            _populate_rate_codes()
        elif _choice == "7":
            _populate_zones()
        elif _choice == "8":
                # kysytään vuosi
            _y = input("Anna vuosi:  ")
            # ja kuukausi
            _m = input("Anna kuukausi:  ")
            # ladataan parquet-tiedosto
            file = download_taxi_data(int(_y), int(_m))
            #import datetime
            #start = datetime.datetime.now()

            start = datetime.now()
            process_and_copy_yellow_trips(file)
            end = datetime.now()

            print(f"Aikaa kulunut: {end - start}")
            #clean_yellow_trips()

    print("\ndone")



if __name__ == "__main__":
    run()