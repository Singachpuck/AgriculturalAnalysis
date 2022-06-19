from configparser import ConfigParser
import psycopg2
from psycopg2 import extensions


class dataaccessor:
    DUMMY_SECTION = 'DUMMY_SECTION'

    def __init__(self, *, filename=None, username=None, password=None, database=None):
        if filename is not None:
            with open(filename, 'r') as f:
                config_string = f'[{dataaccessor.DUMMY_SECTION}]\n{f.read()}'
            cp = ConfigParser()
            cp.read_string(config_string)
            self.username = cp[dataaccessor.DUMMY_SECTION]['username']
            self.password = cp[dataaccessor.DUMMY_SECTION]['password']
            self.database = cp[dataaccessor.DUMMY_SECTION]['database']
        else:
            self.username = username
            self.password = password
            self.database = database

        self.__check_connection()
    
    def __get_connection(self):
        return psycopg2.connect(host='localhost',
                                port=5432,
                                database=self.database,
                                user=self.username,
                                password=self.password)
    
    def __check_connection(self):
        conn = self.__get_connection()
        conn.close()

    def extract_data(self, table_name):
        conn = self.__get_connection()
        try:
            cursor = conn.cursor()
            
            cursor.execute(f"select * from {extensions.quote_ident(table_name, conn)}")

            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            return columns, data
        finally:
            conn.close()


if __name__ == '__main__':
    da = dataaccessor(filename='cred.properties')

    print(da.extract_data('see_all'))
