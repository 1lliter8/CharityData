from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import IntegrityError
import pw
import bcp
import os
import io
import csv
from datetime import datetime


class PopulateEW:
    def __init__(self, wd, host, db, user, password):
        self.eng = 'postgresql+psycopg2://{us}:{pw}@{hs}:5432/{db}'.\
            format(us=user, pw=password, hs=host, db=db)
        self.engine = create_engine(self.eng)
        self.md = MetaData()
        self.md.reflect(bind=self.engine)
        self.wd = wd

    def bcptosql(self, file, name, source, index=False):
        # Clears anything in the table with a different source code
        d = self.md.tables[name]\
            .delete(bind=self.engine)\
            .where(self.md.tables[name].c.source_key != source)
        d.execute()

        # Reads BCP file to csv StringIO buffer
        fbuf = io.StringIO()
        count = 0
        with open(self.wd + file, 'r', encoding='latin-1') as f:
            bcp_stream = bcp.reader(f)
            csvw = csv.writer(fbuf, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for i in bcp_stream:
                i = [sub.replace('\n', '').
                         replace('\t', '').
                         replace('\0', '').
                         replace('"', '').
                         rstrip() for sub in i]
                if index:
                    o = [count] + i
                    count += 1
                else:
                    o = i
                o.append(source)
                csvw.writerow(o)

        # Reads from buffer and uploads
        try:
            fbuf.seek(0)
            sql_cnxn = self.engine.raw_connection()
            cursor = sql_cnxn.cursor()
            cursor.copy_from(fbuf, name, sep='\t', null='')
            sql_cnxn.commit()
            cursor.close()
            print('{file}, {time}: Successfully inserted'.format(file=file,
                                                                 time=datetime.now()))
        except IntegrityError as error:
            error_trunc = str(error).partition('\n')[0]
            readout = '{file}, {time}: {error}'.format(file=file,
                                                       time=datetime.now(),
                                                       error=error_trunc)
            with open('docs/log.txt', 'a') as f2:
                f2.write('\n' + readout)
            print(readout)

    def gensource(self, source):
        # Generates a source row with a string and returns its key
        s = self.md.tables['data_source']
        s = s.insert(bind=self.engine).\
            values(source_name=source)
        key = s.execute()
        return key.inserted_primary_key[0]

    def bcpimports(self):
        # Runs all working BCP import scripts
        source = self.gensource(os.path.split(os.path.dirname(self.wd))[-1])

        self.bcptosql(file='extract_aoo_ref.bcp',
                      name='ew_aoo_ref',
                      source=source)

        self.bcptosql(file='extract_charity.bcp',
                      name='ew_charity',
                      source=source)

        self.bcptosql(file='extract_name.bcp',
                      name='ew_name',
                      source=source)

        self.bcptosql(file='extract_main_charity.bcp',
                      name='ew_main_charity',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_class_ref.bcp',
                      name='ew_class_ref',
                      source=source)

        self.bcptosql(file='extract_remove_ref.bcp',
                      name='ew_remove_ref',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_acct_submit.bcp',
                      name='ew_acct_submit',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_ar_submit.bcp',
                      name='ew_ar_submit',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_charity_aoo.bcp',
                      name='ew_charity_aoo',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_class.bcp',
                      name='ew_class',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_financial.bcp',
                      name='ew_financial',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_objects.bcp',
                      name='ew_objects',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_partb.bcp',
                      name='ew_partb',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_registration.bcp',
                      name='ew_registration',
                      source=source,
                      index=True)

        self.bcptosql(file='extract_trustee.bcp',
                      name='ew_trustee',
                      source=source,
                      index=True)


if __name__ == '__main__':
    # TODO: Test data integrity with some queries, start work on Scotland tables
    os.chdir('..')
    pop = PopulateEW('downloads/RegPlusExtract_May_2020/',
                      'localhost', 'CCEng', pw.us, pw.pw)

    print('Import begun at {time}'.format(time=datetime.now()))
    pop.bcpimports()
    print('Import finished at {time}'.format(time=datetime.now()))