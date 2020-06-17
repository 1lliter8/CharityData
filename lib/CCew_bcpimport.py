from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import IntegrityError
import pandas as pd
import numpy as np
import pw
import bcp
import os
from datetime import datetime

class PopulateEW:
    def __init__(self, wd, host, db, user, password):
        eng = 'postgresql+psycopg2://{us}:{pw}@{hs}:5432/{db}'
        self.engine = create_engine(eng.format(us=user, pw=password, hs=host, db=db))
        self.md = MetaData()
        self.md.reflect(bind=self.engine)
        self.wd = wd

    def bcptosql(self, file, name, headers, source, index=False):
        # Clears anything in the table with a different source code
        d = self.md.tables[name]\
            .delete(bind=self.engine)\
            .where(self.md.tables[name].c.source_key != source)
        d.execute()

        # Reads BCP file to pandas dataframe and uploads
        bcp_items = []
        with open(self.wd + file, 'r', encoding='latin-1') as f:
            bcp_stream = bcp.DictReader(f, fieldnames=headers)
            for i in bcp_stream:
                bcp_items.append(i)

        df = pd.DataFrame.from_dict(bcp_items)
        df = df.replace(r'^\s*$', np.NaN, regex=True)
        df = df.replace('\\x00', np.NaN, regex=True)
        df['source_key'] = source
        df.index.name = 'id'

        try:
            df.to_sql(name, con=self.engine, if_exists='append', index=index)
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
                    headers=['aootype', 'aookey', 'aooname', 'aoosort', 'welsh', 'master'],
                    source=source)

        self.bcptosql(file='extract_charity.bcp',
                    name='ew_charity',
                    headers=['regno', 'subno', 'name', 'orgtype', 'gd', 'aob', 'aob_defined', 'nhs',
                             'ha_no', 'corr', 'add1', 'add2', 'add3', 'add4', 'add5', 'postcode',
                             'phone', 'fax'],
                    source=source)

        self.bcptosql(file='extract_name.bcp',
                    name='ew_name',
                    headers=['regno', 'subno', 'nameno', 'name'],
                    source=source)

        self.bcptosql(file='extract_main_charity.bcp',
                    name='ew_main_charity',
                    headers=['regno', 'coyno', 'trustees', 'fyend', 'welsh', 'incomedate', 'income',
                             'grouptype', 'email', 'web'],
                    source=source,
                    index=True)

        self.bcptosql(file='extract_class_ref.bcp',
                    name='ew_class_ref',
                    headers=['classno', 'classtext'],
                    source=source)

        self.bcptosql(file='extract_remove_ref.bcp',
                    name='ew_remove_ref',
                    headers=['code', 'text'],
                    source=source)

        self.bcptosql(file='extract_acct_submit.bcp',
                    name='ew_acct_submit',
                    headers=['regno', 'submit_date', 'arno', 'fyend'],
                    source=source,
                    index=True)

        self.bcptosql(file='extract_ar_submit.bcp',
                    name='ew_ar_submit',
                    headers=['regno', 'arno', 'submit_date'],
                    source=source,
                    index=True)


if __name__ == '__main__':
    # TODO: Test missing imports
    #   Consider having the main class write those methods to a list then iterating to run
    #   That way you can show progress better?
    #   Probably should query the table for stuff existing before doing all the heavy lifting of
    #   building the df
    os.chdir('..')
    pop = PopulateEW('downloads/RegPlusExtract_May_2020/',
                      'localhost', 'CCEng', pw.us, pw.pw)
    # pop.bcpimports()

    # Start here tomorrow
    """
    pop.bcptosql(file='extract_charity_aoo.bcp',
                name='ew_charity_aoo',
                headers=['regno', 'aootype', 'aookey', 'welsh', 'master'],
                source=4,
                index=True)
    """
    """
    pop.bcptosql(file='extract_class.bcp',
                name='ew_class',
                headers=['regno', 'ewclass'],
                source=4,
                index=True)
    """
    """
    pop.bcptosql(file='extract_financial.bcp',
                name='ew_financial',
                headers=['regno', 'fystart', 'fyend', 'income', 'expend'],
                source=4,
                index=True)
    """
    """
    pop.bcptosql(file='extract_objects.bcp',
                name='ew_objects',
                headers=['regno', 'subno', 'seqno', 'object', 'expend'],
                source=4,
                index=True)
    """
    """
    pop.bcptosql(file='extract_overseas_expend.bcp',
                name='ew_overseas_expend',
                headers=['regno', 'artype', 'fystart', 'fyend', 'overseas_expend', 'inc_leg',
                         'inc_end', 'inc_vol', 'inc_fr', 'inc_char', 'inc_invest', 'inc_other',
                         'inc_total', 'invest_gain', 'asset_gain', 'pension_gain', 'exp_vol',
                         'exp_trade', 'exp_invest', 'exp_grant', 'exp_charble', 'exp_gov',
                         'exp_other', 'exp_total', 'exp_support', 'exp_dep', 'reserves', 'asset_open',
                         'asset_close', 'fixed_assets', 'open_assets', 'invest_assets', 'cash_assets',
                         'current_assets', 'credit_1', 'credit_long', 'pension_assets', 'total_assets',
                         'funds_end', 'funds_restrict', 'funds_unrestrict', 'funds_total', 'employees',
                         'volunteers', 'cons_acc', 'charity_acc'],
                source=4,
                index=True)
    """
    """
    pop.bcptosql(file='extract_partb.bcp',
                name='ew_partb',
                headers=['regno', 'artype', 'fystart', 'fyend', 'inc_leg',
                         'inc_end', 'inc_vol', 'inc_fr', 'inc_char', 'inc_invest', 'inc_other',
                         'inc_total', 'invest_gain', 'asset_gain', 'pension_gain', 'exp_vol',
                         'exp_trade', 'exp_invest', 'exp_grant', 'exp_charble', 'exp_gov',
                         'exp_other', 'exp_total', 'exp_support', 'exp_dep', 'reserves', 'asset_open',
                         'asset_close', 'fixed_assets', 'open_assets', 'invest_assets', 'cash_assets',
                         'current_assets', 'credit_1', 'credit_long', 'pension_assets', 'total_assets',
                         'funds_end', 'funds_restrict', 'funds_unrestrict', 'funds_total', 'employees',
                         'volunteers', 'cons_acc', 'charity_acc'],
                source=4,
                index=True)
    """
    """
    pop.bcptosql(file='extract_registration.bcp',
                name='ew_registration',
                headers=['regno', 'subno', 'regdate', 'remdate', 'remcode'],
                source=4,
                index=True)
    """
    """
    pop.bcptosql(file='extract_trustee.bcp',
                name='ew_trustee',
                headers=['regno', 'trustee'],
                source=4,
                index=True)
    """