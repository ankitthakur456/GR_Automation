import requests
import schedule
import time
import struct
from pyModbusTCP.client import ModbusClient
import os
import datetime
import sys
from database import DBHelper
import logging.handlers
from logging.handlers import TimedRotatingFileHandler

# Setting up Rotating file logging
if getattr(sys, 'frozen', False):
    dirname = os.path.dirname(sys.executable)
else:
    dirname = os.path.dirname(os.path.abspath(__file__))

log_level = logging.INFO

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s'

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("LOGS")

# Checking and creating logs directory here
log_dir = os.path.join(dirname, 'logs')
if not os.path.isdir(log_dir):
    log.info("[-] logs directory doesn't exist")
    try:
        os.mkdir(log_dir)
        log.info("[+] Created logs dir successfully")
    except Exception as e:
        log.error(f"[-] Can't create dir logs Error: {e}")

fileHandler = TimedRotatingFileHandler(os.path.join(log_dir, 'app_log'),
                                       when='midnight', interval=1)

fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)
# endregion

#  Every -- Seconds send data to server
sample_rate = 10
# To Stop sending data to server
send_data = True
headers = {"Content-Type": 'application/json'}
c = DBHelper()
machine_info = {
    'METER-9': {
        'type_': 'EM',
        'unitId': 9,
        'start_reg': 1,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "QApjew1KXdKIA4qZe60f"
    },
    'METER-10': {
        'type_': 'EM',
        'unitId': 10,
        'start_reg': 21,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "tXLmC1my3Hi9JCfUCDtW"
    },
    'METER-11': {
        'type_': 'EM',
        'unitId': 11,
        'start_reg': 41,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "9lDlLm0NuCHx5YlsTffB"
    },
    'METER-12': {
        'type_': 'EM',
        'unitId': 12,
        'start_reg': 61,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "4OJj0pXn1UjOtsba9PDs"
    },
    'METER-13': {
        'type_': 'EM',
        'unitId': 13,
        'start_reg': 81,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "k6VHLhqLowMYob5NFj6d"
    },
    'METER-14': {
        'type_': 'EM',
        'unitId': 14,
        'start_reg': 101,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "CGEswCQfNf1GqLrR5T5q"
    }}
machine_info_1 = {
    'METER-15': {
        'type_': 'EM',
        'unitId': 15,
        'start_reg': 121,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "tTOI6ksLNcPr68bvka4O"
    },
    'METER-16': {
        'type_': 'EM',
        'unitId': 16,
        'start_reg': 141,
        'reg_length': 19,
        'pName': ['PHASE-1_CURRENT', 'PHASE-2_CURRENT', 'PHASE-3_CURRENT', 'NEUTRAL_CURRENT', 'AVERAGE_VALUE_CURRENT',
                  'VOLTAGE_V12', 'VOLTAGE_V23', 'VOLTAGE_V31', 'AVERAGE_VALUE_VOLTAGE(L-L)', 'VOLTAGE_V1N',
                  'VOLTAGE_V1N',
                  'VOLTAGE_V1N', 'AVERAGE_VALUE_VOLTAGE(L-N)', 'POWER_FACTOR', 'FREQUENCY', 'kWh', 'kVAh'
                  ],
        'access_token': "Nh9SoADeb8H3xinbCgAS"
    }}



def initiate_client(ip):
    """returns the modbus client instance"""
    log.info(f'Modbus Client IP: {ip}')
    return ModbusClient(host=ip, port=502, unit_id=1, auto_open=True, auto_close=True, timeout=2)


def decode_ieee(val_int):
    return struct.unpack("f", struct.pack("I", val_int))[0]


def word_list_to_long(val_list, big_endian=True):
    # allocate list for long int
    long_list = [None] * int(len(val_list) / 2)
    # fill registers list with register items
    for i, item in enumerate(long_list):
        if big_endian:
            long_list[i] = (val_list[i * 2] << 16) + val_list[(i * 2) + 1]
        else:
            long_list[i] = (val_list[(i * 2) + 1] << 16) + val_list[i * 2]
    # return long list
    return long_list


def f_list(values, bit=True):
    fist = []
    for f in word_list_to_long(values, bit):
        fist.append(round(decode_ieee(f), 3))
    # log.info(len(f_list),f_list)
    return fist


def convert(reg2, reg1):
    complete_value = (reg1 << 16) | reg2
    # log.info or use the complete value
    log.info("Complete Value:", complete_value)
    return complete_value


def post_data(payload, access_token):
    """posting OEE DATA to the SERVER"""
    url = f'http://localhost:8080/api/v1/{access_token}/telemetry'
    log.info("[+] Sending data to server")
    if send_data:
        try:
            send_req = requests.post(url, json=payload, headers=headers, timeout=2)
            log.info(send_req.status_code)
            send_req.raise_for_status()
            sync_data = c.get_sync_data()
            if sync_data:
                for i in sync_data:
                    if i:
                        machine_id_sync = i.get('machine_id')
                        payload = i.get('payload')
                        log.info(f"[+] ----Machine_ID-----{machine_id_sync}")
                        for sync_payload in payload:
                            max_ts = max([j['ts'] for j in sync_payload])
                            try:
                                url = f'http://localhost:8080/api/v1/{machine_info[machine_id_sync]["access_token"]}/' \
                                      f'telemetry'
                                log.info(url)
                                sync_req = requests.post(url, json=sync_payload, headers=headers, timeout=2)
                                sync_req.raise_for_status()
                                log.info(f"[+] Sync_successful for timestamp less or equal to {max_ts}")
                                # clearing sync_payloads only when data is synced successfully
                                c.clear_sync_data(max_ts, machine_id_sync)
                                with open(os.path.join(dirname, f'logs/sync_log{datetime.date.today()}.txt'), 'a') as f:
                                    pname = f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ---- SYNC DONE\n'
                                    f.write(pname)
                                time.sleep(0.1)
                            except Exception as er:
                                log.error(f"[-] Error in sending SYNC Cycle time data {er}")
                    else:
                        log.info("(^-^) No data to sync")
                        break

        except Exception as er:
            log.error(f"[-] Error in sending Cycle time data {er}")
            c.add_sync_data(payload, 1)


def get_ems_values():
    for i in range(2):
        try:
            log.info("[+] Reading values from holding registers")
            # Read from first device
            mb_client = initiate_client('172.16.29.164')
            data0 = mb_client.read_holding_registers(0, 120)
            log.info(data0)
            # Close connection to first device
            mb_client.close()
            # Read from second device

            if data0:
                log.info(f'data0 is {data0}')
                data0_processed = []
                # Process data in groups of 20
                for i in range(0, len(data0), 20):
                    group = data0[i:i + 20]
                    processed_group = [
                        value // 10 if j not in [15, 16] else value
                        for j, value in enumerate(group)
                    ]
                    data0_processed.extend(processed_group)
                grouped_values = [data0_processed[i:i + 20] for i in range(0, len(data0_processed), 20)]

                for group, (j, k) in zip(grouped_values, machine_info.items()):
                    result_dict = {}
                    reg1 = group[15]  # Replace with your Modbus read function
                    log.info(f'register 1 for kwh {reg1}')
                    reg2 = group[16]  # Replace with your Modbus read function
                    log.info(f'register 2 for kwh {reg2}')
                    value1 = convert(reg1, reg2)
                    reg3 = group[17]
                    reg4 = group[18]
                    new_data = group[:15]
                    value2 = convert(reg3, reg4)
                    new_data.append(value1)
                    new_data.append(value2)
                    values = new_data
                    keys = k['pName']
                    log.info(f'Keys IS {keys}')
                    m_name = j
                    log.info(f'machine name is {m_name}')
                    access_token = k['access_token']
                    log.info(f'ACCESS TOKEN IS {access_token}')

                    result_dict = {key: value for key, value in zip(keys, values)}
                    log.info(result_dict)
                    payload = {}
                    if result_dict is None:
                        post_mb_error(m_name, access_token, True)
                        payload['machine_running'] = False
                        post_data(payload, access_token)
                        log.info(payload)
                        time.sleep(2)
                    else:
                        result_dict['machine_running'] = True
                        post_data(result_dict, access_token)
                        log.info(payload)
                        time.sleep(5)

            mb_client1 = initiate_client('172.16.29.164')
            data1 = mb_client1.read_holding_registers(120, 40)
            log.info(data1)
            # Close connection to second device
            mb_client1.close()
            if data1:
                data1_processed = []
                # Process data in groups of 20
                for i in range(0, len(data1), 20):
                    group = data1[i:i + 20]
                    processed_group = [
                        value // 10 if j not in [15, 16] else value
                        for j, value in enumerate(group)
                    ]

                    data1_processed.extend(processed_group)
                grouped_values_1 = [data1_processed[i:i + 20] for i in range(0, len(data1_processed), 20)]
                for group, (j, k) in zip(grouped_values_1, machine_info_1.items()):
                    result_dict = {}
                    reg1 = group[15]  # Replace with your Modbus read function
                    print(f'register 1 for kwh {reg1}')
                    reg2 = group[16]  # Replace with your Modbus read function
                    print(f'register 2 for kwh {reg2}')
                    value1 = convert(reg1, reg2)
                    reg3 = group[17]
                    reg4 = group[18]
                    new_data = group[:15]
                    value2 = convert(reg3, reg4)
                    new_data.append(value1)
                    new_data.append(value2)
                    values = new_data
                    keys = k['pName']
                    result_dict = {key: value for key, value in zip(keys, values)}
                    log.info(result_dict)
                    m_name = j
                    access_token = k['access_token']
                    log.info(f'machine name is {m_name}')
                    log.info(f'ACCESS TOKEN IS {access_token}')
                    log.info(f'Keys IS {keys}')
                    payload = {}
                    if result_dict is None:
                        post_mb_error(m_name, access_token, True)
                        payload['machine_running'] = False
                        post_data(payload,access_token)
                        log.info(payload)
                        time.sleep(2)
                    else:
                        result_dict['machine_running'] = True
                        post_data(result_dict, access_token)
                        log.info(payload)
                        time.sleep(5)
        except Exception as er:
            log.error(f"ERROR:{er}")
            time.sleep(5)

schedule.every(sample_rate).seconds.do(get_ems_values)


def post_mb_error(m_name, accessToken, status):
    """posting an error in the attributes if the data is None"""
    global headers

    url = f'http://localhost:8080/api/v1/{accessToken}/attributes'

    payload = {"error": status}
    log.info(f'"machineId:" {m_name}' + str(payload))
    if send_data:
        request_response = requests.post(url, json=payload, headers=headers, timeout=5)
        log.info(request_response.text)


try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    pass
