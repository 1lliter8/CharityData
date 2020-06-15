from sqlalchemy import create_engine, MetaData
import pandas as pd
import numpy as np
import pw
import bcp

class PopulateEW:
    def __init__(self, wd, host, db, user, password):
        eng = 'postgresql+psycopg2://{us}:{pw}@{hs}:5432/{db}'
        self.engine = create_engine(eng.format(us=user, pw=password, hs=host, db=db))
        self.md = MetaData(bind=self.engine, reflect=True)
        self.wd = wd

    def bcptosql(self, file, name, headers, source):
        # TODO: Add a way to delete all values from the table if source isn't this one
        #   https://gist.github.com/absent1706/3ccc1722ea3ca23a5cf54821dbc813fb
        with open(self.wd + file, 'r') as f:
            bcp_stream = bcp.DictReader(f, fieldnames=headers)
            bcp_working = {}
            bcp_clean = dict.fromkeys(headers, [])

            for i in bcp_stream:
                bcp_working = {k: list(bcp_clean[k] + [i[k]]) for k in bcp_clean}
                bcp_clean = bcp_working

            df = pd.DataFrame.from_dict(bcp_clean)
            df = df.replace(r'^\s*$', np.NaN, regex=True)
            df['source_key'] = source
            df.to_sql(name, con=self.engine, if_exists='append', index=False)

if __name__ == '__main__':
    pop = PopulateEW('C:\\Users\\willl\\PycharmProjects\\CharityData\\downloads\\RegPlusExtract_May_2020\\',
                      'localhost', 'CCEng', pw.us, pw.pw)

    pop.bcptosql(file='extract_aoo_ref.bcp',
                name='ew_aoo_ref',
                headers=['aootype', 'aookey', 'aooname', 'aoosort', 'welsh', 'master'],
                source=1)

    # TODO: Start running other tables then see if/why/how they take ages

    # TODO: Implement data_source for versioning, potentially move functions into class?
    #   So you'd call definesource() then importbcp()
    #   Then maybe call them both in one function so this whole script can be called

    """
    pop.bcptosql(file='extract_main_charity.bcp',
                name='ew_main_charity',
                headers=['regno', 'coyno', 'trustees', 'fyend', 'welsh', 'incomedate', 'income',
                         'grouptype', 'email', 'web'],
                source=1)
    """
    """
    pop.bcptosql(file='extract_name.bcp',
                name='ew_name',
                headers=['regno', 'subno', 'nameno', 'name'],
                source=1)
    """
    """
    pop.bcptosql(file='extract_class_ref.bcp',
                name='ew_class_ref',
                headers=['classno', 'classtext'],
                source=1)
    """
    """
    pop.bcptosql(file='extract_remove_ref.bcp',
                name='ew_remove_ref',
                headers=['code', 'text'],
                source=1)
    """
    # TODO: Files from here on need a primary key - an ID
    #   Could use a boolean for index=True/False on pd.to_sql
    """
    pop.bcptosql(file='extract_acct_submit.bcp',
                name='ew_acct_submit',
                headers=['regno', 'submit_date', 'arno', 'fyend'],
                source=1)
    id = Column(Integer, primary_key=True)
    regno = Column(Integer, ForeignKey('ew_main_charity.regno'))
    submit_date = Column(TIMESTAMP)
    arno = Column(String(4), nullable=False)
    fyend = Column(String(4))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)
    """
