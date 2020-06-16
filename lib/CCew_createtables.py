from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, ForeignKey, ForeignKeyConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import pw
import os

Base = declarative_base()


class Data_Source(Base):
    __tablename__ = 'data_source'

    source_key = Column(Integer, primary_key=True)
    source_name = Column(String(255), nullable=False)
    source_date = Column(TIMESTAMP, nullable=False, default=func.now())


class EW_Charity(Base):
    __tablename__ = 'ew_charity'

    regno = Column(Integer, primary_key=True)
    subno = Column(Integer, primary_key=True)
    name = Column(String(150))
    orgtype = Column(String(10))
    gd = Column(Text)
    aob = Column(Text)
    aob_defined = Column(Integer)
    nhs = Column(String(1))
    ha_no = Column(Integer)
    corr = Column(String(255))
    add1 = Column(String(35))
    add2 = Column(String(35))
    add3 = Column(String(35))
    add4 = Column(String(35))
    add5 = Column(String(35))
    postcode = Column(String(8))
    phone = Column(String(400))
    fax = Column(Integer)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class EW_Name(Base):
    __tablename__ = 'ew_name'

    regno = Column(Integer)
    subno = Column(Integer)
    nameno = Column(Integer, primary_key=True)
    name = Column(String(255))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)

    ForeignKeyConstraint(['regno', 'subno'], ['ew_charity.regno', 'ew_charity.subno'])


class EW_Main_Charity(Base):
    __tablename__ = 'ew_main_charity'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    coyno = Column(String(50))
    trustees = Column(String(1), nullable=False)
    fyend = Column(String(4))
    welsh = Column(String(1), nullable=False)
    incomedate = Column(TIMESTAMP)
    income = Column(Integer)
    grouptype = Column(String(3))
    email = Column(String(400))
    web = Column(String(400))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)



class EW_Aoo_Ref(Base):
    __tablename__ = 'ew_aoo_ref'

    aootype = Column(String(10), nullable=False, primary_key=True)
    aookey = Column(Integer, nullable=False, primary_key=True)
    aooname = Column(String(255), nullable=False)
    aoosort = Column(String(100), nullable=False)
    welsh = Column(String(1), nullable=False)
    master = Column(Integer)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class EW_Class_Ref(Base):
    __tablename__ = 'ew_class_ref'

    classno = Column(String(10), primary_key=True)
    classtext = Column(String(65))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class EW_Remove_Ref(Base):
    __tablename__ = 'ew_remove_ref'

    code = Column(String(3), primary_key=True)
    text = Column(String(25))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class EW_Acct_Submit(Base):
    __tablename__ = 'ew_acct_submit'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    submit_date = Column(TIMESTAMP)
    arno = Column(String(4), nullable=False)
    fyend = Column(String(4))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class EW_Ar_Submit(Base):
    __tablename__ = 'ew_ar_submit'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    arno = Column(String(4), nullable=False)
    submit_date = Column(TIMESTAMP)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class EW_Charity_Aoo(Base):
    __tablename__ = 'ew_charity_aoo'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    aootype = Column(String(10), nullable=False)
    aookey = Column(Integer, nullable=False)
    welsh = Column(String(1), nullable=False)
    master = Column(Integer)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)

    ForeignKeyConstraint(['aootype', 'aookey'], ['ew_aoo_ref.aootype', 'ew_aoo_ref.aookey'])


class EW_Class(Base):
    __tablename__ = 'ew_class'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    ewclass = Column(String(10), ForeignKey('ew_class_ref.classno'), nullable=False)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class EW_Financial(Base):
    __tablename__ = 'ew_financial'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    fystart = Column(TIMESTAMP)
    fyend = Column(TIMESTAMP)
    income = Column(Integer)
    expend = Column(Integer)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)



class EW_Objects(Base):
    __tablename__ = 'ew_objects'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    subno = Column(Integer)
    seqno = Column(String(4))
    object = Column(Text)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)

    ForeignKeyConstraint(['regno', 'subno'], ['ew_charity.regno', 'ew_charity.subno'])


class EW_Overseas_Expend(Base):
    __tablename__ = 'ew_overseas_expend'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    artype = Column(String(4), nullable=False)
    fystart = Column(TIMESTAMP, nullable=False)
    fyend = Column(TIMESTAMP, nullable=False)
    overseas_expend = Column(Text)
    inc_leg = Column(Text)
    inc_end = Column(Text)
    inc_vol = Column(Text)
    inc_fr = Column(Text)
    inc_char = Column(Text)
    inc_invest = Column(Text)
    inc_other = Column(Text)
    inc_total = Column(Text)
    invest_gain = Column(Text)
    asset_gain = Column(Text)
    pension_gain = Column(Text)
    exp_vol = Column(Text)
    exp_trade = Column(Text)
    exp_invest = Column(Text)
    exp_grant = Column(Text)
    exp_charble = Column(Text)
    exp_gov = Column(Text)
    exp_other = Column(Text)
    exp_total = Column(Text)
    exp_support = Column(Text)
    exp_dep = Column(Text)
    reserves = Column(Text)
    asset_open = Column(Text)
    asset_close = Column(Text)
    fixed_assets = Column(Text)
    open_assets = Column(Text)
    invest_assets = Column(Text)
    cash_assets = Column(Text)
    current_assets = Column(Text)
    credit_1 = Column(Text)
    credit_long = Column(Text)
    pension_assets = Column(Text)
    total_assets = Column(Text)
    funds_end = Column(Text)
    funds_restrict = Column(Text)
    funds_unrestrict = Column(Text)
    funds_total = Column(Text)
    employees = Column(Text)
    volunteers = Column(Text)
    cons_acc = Column(Text)
    charity_acc = Column(Text)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)



class EW_PartB(Base):
    __tablename__ = 'ew_partb'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    artype = Column(String(4), nullable=False)
    fystart = Column(TIMESTAMP, nullable=False)
    fyend = Column(TIMESTAMP, nullable=False)
    inc_leg = Column(Text)
    inc_end = Column(Text)
    inc_vol = Column(Text)
    inc_fr = Column(Text)
    inc_char = Column(Text)
    inc_invest = Column(Text)
    inc_other = Column(Text)
    inc_total = Column(Text)
    invest_gain = Column(Text)
    asset_gain = Column(Text)
    pension_gain = Column(Text)
    exp_vol = Column(Text)
    exp_trade = Column(Text)
    exp_invest = Column(Text)
    exp_grant = Column(Text)
    exp_charble = Column(Text)
    exp_gov = Column(Text)
    exp_other = Column(Text)
    exp_total = Column(Text)
    exp_support = Column(Text)
    exp_dep = Column(Text)
    reserves = Column(Text)
    asset_open = Column(Text)
    asset_close = Column(Text)
    fixed_assets = Column(Text)
    open_assets = Column(Text)
    invest_assets = Column(Text)
    cash_assets = Column(Text)
    current_assets = Column(Text)
    credit_1 = Column(Text)
    credit_long = Column(Text)
    pension_assets = Column(Text)
    total_assets = Column(Text)
    funds_end = Column(Text)
    funds_restrict = Column(Text)
    funds_unrestrict = Column(Text)
    funds_total = Column(Text)
    employees = Column(Text)
    volunteers = Column(Text)
    cons_acc = Column(Text)
    charity_acc = Column(Text)
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)



class EW_Registration(Base):
    __tablename__ = 'ew_registration'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    subno = Column(Integer)
    regdate = Column(TIMESTAMP)
    remdate = Column(TIMESTAMP)
    remcode = Column(String(3), ForeignKey('ew_remove_ref.code'))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)

    ForeignKeyConstraint(['regno', 'subno'], ['ew_charity.regno', 'ew_charity.subno'])


class EW_Trustee(Base):
    __tablename__ = 'ew_trustee'

    id = Column(Integer, primary_key=True)
    regno = Column(Integer)
    trustee = Column(String(255))
    source_key = Column(Integer, ForeignKey('data_source.source_key'), nullable=False)


class BuildDB:
    def __init__(self, wd, host, db, user, password):
        eng = 'postgresql+psycopg2://{us}:{pw}@{hs}:5432/{db}'
        self.engine = create_engine(eng.format(us=user, pw=password, hs=host, db=db))
        self.Base = Base
        self.md = self.Base.metadata
        self.md.create_all(self.engine)
        self.wd = wd


if __name__ == '__main__':
    os.chdir('..')
    build = BuildDB('downloads/RegPlusExtract_May_2020',
                     'localhost', 'CCEng', pw.us, pw.pw)
