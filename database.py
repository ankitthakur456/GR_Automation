import datetime
import ast
import sqlite3
import time
import math
import logging
log = logging.getLogger("Logs")

class DBHelper:
    def __init__(self):
        self.connection = sqlite3.connect("HIS.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS sync_data(ts INTEGER, payload STRING, machine_id STRING)")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS reset_energy(id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp REAL, energy_at_7am STRING)""")

    # region Syncronization functions
    def add_sync_data(self, payload, machine_id):
        try:
            ts = int(time.time() * 1000)
            new_payload = dict()
            for i in payload.items():
                if math.isnan(i[1]):
                    data = 'nan'
                else:
                    data = i[1]
                new_payload[i[0]] = data
            print(new_payload)
            # ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute('''INSERT INTO sync_data(ts, payload, machine_id) VALUES (?,?,?)''',
                                (ts, str(new_payload), machine_id))
            print('Successful Sync Payload Added to the database')
            self.connection.commit()
        except Exception as e:
            print(f'ERROR {e} Sync Data not added to the database')

    def get_sync_data(self):
        try:
            sync_payload = list()
            self.cursor.execute('''SELECT machine_id FROM sync_data group by machine_id''')
            machine_ids = self.cursor.fetchall()
            print(machine_ids)
            if machine_ids is not None:
                for at in machine_ids:
                    if at is not None:
                        self.cursor.execute('''SELECT ts, payload FROM sync_data where machine_id=? order by ts ASC''',
                                            (at[0],))
                        data = self.cursor.fetchall()
                        # print(data)
                        if len(data):
                            data_payload = [{"ts": int(item[0]),
                                             "values": ast.literal_eval(item[1]),
                                             }
                                            for item in data]
                            # splitting data_payload in list of lists of 100 items those 100 items are objects
                            # containing ts and values and returning that list as an object
                            sync_payload.append({
                                "machine_id": at[0],
                                "payload": [data_payload[i:i + 100] for i in range(0, len(data_payload), 100)]
                            })

                return sync_payload
            return []
        except Exception as e:
            print(f'ERROR {e} No Sync Data available')
            return []

    def clear_sync_data(self, ts, machine_id):
        try:
            # deleting the payload where ts is less than or equal to ts
            self.cursor.execute("""DELETE FROM sync_data WHERE ts<=? and machine_id=?""", (ts, machine_id))
            self.connection.commit()
            print(f"Successful, Cleared Sync payload from the database for {ts}")
            return True
        except Exception as e:
            print(f'Error in clear_sync_data {e} No sync Data to clear')
            return False

    def enqueue_energy_at_7am(self, meter_id, energy_at_7am):
        try:
            # Check if the combination of meter_id and energy_at_7am already exists in the reset_energy
            self.cursor.execute(
                """SELECT id FROM reset_energy WHERE meter_id = ? AND energy_at_7am = ?""",
                (meter_id, energy_at_7am))

            existing_record_id = self.cursor.fetchone()

            if existing_record_id is None:
                # If not exists, insert the data into the reset_energy table
                self.cursor.execute(
                    """INSERT INTO reset_energy(meter_id, energy_at_7am, timestamp) VALUES(?,?,?)""",
                    (meter_id, energy_at_7am, time.time()))
                # Commit the changes to the database
                self.connection.commit()

                log.info(f"[+] Successful, Energy from Meter {meter_id} Enqueued to the database")
            else:
                # If exists, update the timestamp of the existing record
                self.cursor.execute(
                    """UPDATE reset_energy SET timestamp = ? WHERE id = ?""",
                    (time.time(), existing_record_id[0]))
                # Commit the changes to the database
                self.connection.commit()

                log.info(f"[+] Successful, Energy from Meter {meter_id} Updated in the database")
        except Exception as e:
            log.error(f"[-] Failed to enqueue energy. Error: {e}")

    def get_or_insert_energy_at_7am(self, meter_id, value):
        try:
            # Check if a record for the current date and meter_id exists
            current_date = datetime.date.today()
            self.cursor.execute(
                """SELECT id, energy_at_7am FROM reset_energy WHERE meter_id = ? AND date(timestamp, 'unixepoch') = ?""",
                (meter_id, current_date)
            )
            existing_record = self.cursor.fetchone()

            if existing_record is None:
                # If no record is found, insert a new record with the default value
                self.cursor.execute(
                    """INSERT INTO reset_energy(meter_id, energy_at_7am, timestamp) VALUES(?,?,?)""",
                    (meter_id, value, time.time())
                )
                # Commit the changes to the database
                self.connection.commit()

                log.info(f"[+] Successful, Inserted new record for Meter {meter_id} after 7 am with default value")

                # Return the default value
                return value
            else:
                # If a record is found, return the energy_at_7am value
                return existing_record[1]
        except Exception as e:
            log.error(f"[-] Failed to get or insert energy_at_7am. Error: {e}")
            return None

    def get_energy_at_7am(self, record_id):
        try:
            self.cursor.execute("""SELECT energy_at_7am FROM reset_energy WHERE meter_id = ?""", (record_id,))
            energy_at_7am = self.cursor.fetchone()
            if energy_at_7am:
                return energy_at_7am[0]
            else:
                return None
        except Exception as e:
            log.error(f"[-] Failed to get energy_at_7am. Error: {e}")
            return None

    def delete_row_by_id(self, row_id):
        try:
            # Execute a SQL query to delete a row with a specific ID from the table
            self.cursor.execute(f"DELETE FROM reset_energy WHERE meter_id = ?", (row_id,))

            # Commit the changes to the database
            self.connection.commit()

            print(f"[+] Successful, Deleted row with ID {row_id} from reset_energy")
        except Exception as e:
            print(f"[-] Failed")

    def delete_energy_at_7am(self):
        try:
            # Execute a SQL query to delete all records from the 'reset_energy' table
            self.cursor.execute("""DELETE FROM reset_energy""")

            # Commit the changes to the database
            self.connection.commit()

            # Log a message indicating successful deletion
            log.info("[+] Successful, All Records Deleted from the database")
        except Exception as e:
            # Log an error message if the deletion fails
            log.error(f"[-] Failed to delete records. Error: {e}")
