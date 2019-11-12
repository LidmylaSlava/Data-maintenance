import base64
import io
import json
import logging
import os
import sys
import zlib
from azure.storage.blob import BlockBlobService
from datetime import date, datetime, timedelta
from iso8601 import parse_date
# from postgres_utils import ensure_master_table, get_processing_dates, load_data
# from psycopg2 import connect


log = logging.getLogger(__name__)
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(logging.Formatter('%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s'))
log.addHandler(out_hdlr)
log.setLevel(logging.INFO)


def decode_base64_and_inflate(b64string):
    decoded_data = base64.b64decode(b64string)
    return zlib.decompress(decoded_data, -15)

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)


def process_rates(file):
    with io.open('/Users/liudmyla/Downloads/rates-2.txt', 'r') as rates: # path to txt file
        for line in rates:
            print(line)
            if '_blob' in line:
                for k, v in json.loads(line.split('_blob')[-1]).items():
                    if k == 'srid':
                        data = {
                            'search_id': int(v)
                        }
                    elif k == 'meta':
                        data.update(
                            {
                            'created': str(parse_date(v['Created']).date()),
                            'nights': int(v['Nights'])
                        }
                        )
                        try:
                            if 'HotelKey' in v:
                                data.update(
                                    {
                                        'check_in': str(parse_date(v['HotelKey']['CheckIn']).date()),
                                        'check_out': str(parse_date(v['HotelKey']['CheckOut']).date()),
                                        'hotel_id': int(v['HotelKey']['HotelId']),
                                        'rooms': str(v['HotelKey']['Rooms'])
                                    }
                                )
                            elif 'HotelKeyStr' in v:
                                data.update(
                                    {
                                        'check_in': str(datetime.strptime(v['HotelKeyStr'].split(':')[1], '%Y%m%d').date()),
                                        'check_out': str(datetime.strptime(v['HotelKeyStr'].split(':')[2], '%Y%m%d').date()),
                                        'hotel_id': int(v['HotelKeyStr'].split(':')[0]),
                                        'rooms': str(v['HotelKeyStr'].split(':')[3])
                                    }
                                )
                                print(data)
                            else:
                                log.error("'meta' field contains neither 'HotelKey' nor 'HotelKeyStr' keys")
                                sys.exit(-1)
                        except Exception as e:
                            log.exception("Error occurred while processing 'meta' field")
                            sys.exit(-1)
                    elif k == 'pkgs' and v != 'i44FAA==':
                        try:
                            pkgs = json.loads(decode_base64_and_inflate(v))
                        except Exception as e:
                            log.exception("Could not unpack base64 string")
                            sys.exit(-1)

                        for pkg in pkgs:
                            data.update(
                                {
                                    'supplier': pkg['Supplier']['Name'] if 'Name' in pkg['Supplier'] else pkg['Supplier']
                                }
                            )

                            if 'Rooms' in pkg:
                                for room in range(len(pkg['Rooms'])):
                                    if 'ProviderRoomRawJson' in pkg['Rooms'][room].keys():
                                        room_raw_json = json.loads(pkg['Rooms'][room]['ProviderRoomRawJson'])
                                    else:
                                        room_raw_json = pkg['Rooms'][room]

                                    for n, m in room_raw_json.items():
                                        if n == 'Price' and m is not None:
                                            data.update(
                                                {
                                                    'final_price': m['FinalPrice'],
                                                    'final_tax': m['FinalTax']
                                                }
                                            )
                                            print(data)
                            else:
                                logging.error("'Rooms' field is undefined")
                                sys.exit(-1)


if __name__ == '__main__':
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME', '') # type account_name
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    AZURE_STORAGE_BLOB_NAME = os.getenv('AZURE_STORAGE_BLOB_NAME', '') # .txt
    AZURE_STORAGE_CONTAINER_NAME = os.getenv('AZURE_STORAGE_CONTAINER_NAME', '') # type container name
    print(AZURE_STORAGE_ACCOUNT_KEY)
def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

start_dt = date(2019, 6, 19)
end_dt = date(2019, 6, 21)
processing_dates = [i for i in daterange(start_dt, end_dt)]
if len(processing_dates) > 0:
        log.info("Will process blob files for dates %s", ", ".join(str(d) for d in processing_dates))

        blob = BlockBlobService(
            account_name=AZURE_STORAGE_ACCOUNT_NAME,
            account_key=AZURE_STORAGE_ACCOUNT_KEY
        )

        for dt in processing_dates:
            if blob.exists(container_name=AZURE_STORAGE_CONTAINER_NAME,
                           blob_name=dt.strftime('%Y/%m/%d') + '/' + AZURE_STORAGE_BLOB_NAME):
                with io.open(AZURE_STORAGE_BLOB_NAME, 'wb') as rates_file:
                    log.info("Started downloading blob %s",
                             dt.strftime('%Y/%m/%d') + '/' + AZURE_STORAGE_BLOB_NAME)
                    try:
                        blob.get_blob_to_stream(
                            container_name=AZURE_STORAGE_CONTAINER_NAME,
                            blob_name=dt.strftime('%Y/%m/%d') + '/' + AZURE_STORAGE_BLOB_NAME,
                            stream=rates_file,
                            max_connections=2
                        )
                    except Exception as e:
                        log.exception("Azure Blob Storage is unavailable")
                        sys.exit(-1)

                    process_rates(AZURE_STORAGE_BLOB_NAME)
