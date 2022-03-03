#!/usr/bin/python
# -*- coding: UTF-8 -*-
import datetime
import decimal
import getopt
import json
import uuid
import sys
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, DateTime, DECIMAL, VARCHAR, text, inspect, create_engine, Integer, null, or_


"""
constraints
"""
TRANSACTION_COLLECTION = '交易收款'

TRANSACTION_REPAYMENT = '交易还款'

TRANSACTION_REFUND = '交易退款'

REFUND_DURING_SALE = '售中退款'

FILE_PATH = './dbConfig.json'

START_DATE = ''

END_DATE = ''

"""
model
"""

Base = declarative_base()


class TMallOrder(Base):
    __tablename__ = 'order_tmall'  # 表名称

    订单编号 = Column('订单编号', VARCHAR(25), primary_key=True)
    支付单号 = Column('支付单号', VARCHAR(50))
    支付详情 = Column('支付详情', VARCHAR(255))
    买家应付货款 = Column('买家应付货款', DECIMAL(10, 0))
    买家应付邮费 = Column('买家应付邮费', DECIMAL(10, 0))
    买家支付积分 = Column('买家支付积分', Integer)
    总金额 = Column('总金额', DECIMAL(10, 0))
    返点积分 = Column('返点积分', Integer)
    买家实际支付金额 = Column('买家实际支付金额', DECIMAL(10, 0))
    买家实际支付积分 = Column('买家实际支付积分', Integer)
    订单状态 = Column('订单状态', VARCHAR(255))
    买家留言 = Column('买家留言', VARCHAR(255))
    订单创建时间 = Column('订单创建时间', DateTime)
    订单付款时间 = Column('订单付款时间', DateTime)
    宝贝种类 = Column('宝贝种类', Integer)
    订单备注 = Column('订单备注', LONGTEXT)
    宝贝总数量 = Column('宝贝总数量', Integer)
    店铺Id = Column('店铺Id', VARCHAR(255))
    店铺名称 = Column('店铺名称', VARCHAR(255))
    卖家服务费 = Column('卖家服务费', DECIMAL(10, 0))
    买家服务费 = Column('买家服务费', DECIMAL(10, 0))
    特权订金订单id = Column('特权订金订单id', Integer)
    修改后的sku = Column('修改后的sku', VARCHAR(255))
    异常信息 = Column('异常信息', VARCHAR(255))
    天猫卡券抵扣 = Column('天猫卡券抵扣', VARCHAR(255))
    集分宝抵扣 = Column('集分宝抵扣', VARCHAR(255))
    是否是O2O交易 = Column('是否是O2O交易', VARCHAR(255))
    新零售交易类型 = Column('新零售交易类型', VARCHAR(255))
    新零售导购门店名称 = Column('新零售导购门店名称', VARCHAR(255))
    新零售导购门店id = Column('新零售导购门店id', VARCHAR(255))
    新零售发货门店名称 = Column('新零售发货门店名称', VARCHAR(255))
    新零售发货门店id = Column('新零售发货门店id', VARCHAR(255))
    退款金额 = Column('退款金额', DECIMAL(10, 0))
    确认收货时间 = Column('确认收货时间', DateTime)
    打款商家金额 = Column('打款商家金额', DECIMAL(10, 0))
    直播返现状态 = Column('直播返现状态', VARCHAR(255))
    返现金额 = Column('返现金额', DECIMAL(10, 0))
    延迟发货自动赔付金额 = Column('延迟发货自动赔付金额', DECIMAL(10, 0))
    预售订单 = Column('预售订单', VARCHAR(10))
    发货时间 = Column('发货时间', DateTime)
    商家备忘 = ('商家备忘', LONGTEXT)
    主订单编号 = Column('主订单编号', VARCHAR(255))


class AlipayBill(Base):
    __tablename__ = 'bill_alipay'

    序号 = Column('序号', Integer, primary_key=True)
    入账时间 = Column('入账时间', DateTime)
    支付宝交易号 = Column('支付宝交易号', VARCHAR(50))
    支付宝流水号 = Column('支付宝流水号', VARCHAR(25))
    商户订单号 = Column('商户订单号', VARCHAR(75))
    账务类型 = Column('账务类型', VARCHAR(10))
    账户余额 = Column('账户余额（元）', DECIMAL(10, 0))
    收入 = Column('收入（+元）', DECIMAL(10, 0))
    支出 = Column('支出（-元）', DECIMAL(10, 0))
    服务费 = Column('服务费（元）', DECIMAL(10, 0))
    支付渠道 = Column('支付渠道', VARCHAR(10))
    签约产品 = Column('签约产品', VARCHAR(15))
    对方账户 = Column('对方账户', VARCHAR(50))
    对方名称 = Column('对方名称', VARCHAR(50))
    银行订单号 = Column('银行订单号', VARCHAR(50))
    商品名称 = Column('商品名称', VARCHAR(100))
    备注 = Column('备注', LONGTEXT)
    业务基础订单号 = Column('业务基础订单号', VARCHAR(50))
    业务订单号 = Column('业务订单号', VARCHAR(75))
    业务账单来源 = Column('业务账单来源', VARCHAR(10))
    业务描述 = Column('业务描述', VARCHAR(50))
    付款备注 = Column('付款备注', VARCHAR(10))


class OrderReconciliation(Base):
    __tablename__ = 'order_reconciliation'  # 表名称

    id = Column(VARCHAR(255), primary_key=True, comment='主键')
    platform_id = Column(VARCHAR(255), comment='平台代码')
    platform_name = Column(VARCHAR(255), comment='平台名称')
    shop_id = Column(VARCHAR(255), comment='店铺编号')
    shop_name = Column(VARCHAR(255), comment='店铺名称')
    order_id = Column(VARCHAR(255), comment='订单编号')
    order_date = Column(DateTime, comment='订单日期')
    shipping_date = Column(DateTime, comment='发货日期,出货日期')
    order_type = Column(VARCHAR(255), comment='订单类型')
    order_amount = Column(DECIMAL(18, 3), comment='订单金额')
    oder_discount_amount = Column(DECIMAL(18, 3), comment='折扣金额')
    oder_discount_info = Column(VARCHAR(255), comment='折扣信息')
    order_match_state = Column(VARCHAR(255), comment='账单匹配状态')
    order_payment_status = Column(VARCHAR(255), comment='回款状态')
    order_category = Column(VARCHAR(255), comment='订单种类')
    products_number = Column(Integer, comment='商品数量')
    total_merchandise = Column(DECIMAL(18, 3), comment='商品总额')
    payment_matching = Column(DECIMAL(18, 3), comment='已匹配收入')
    difference_amount = Column(DECIMAL(18, 3), comment='差异金额')
    online_transaction_number = Column(VARCHAR(255), comment='网店交易号')
    latest_reconciliation_status_time = Column(DateTime, comment='对账状态最新时间')
    is_overdue = Column(VARCHAR(255), comment='是否超期')
    is_finish = Column(VARCHAR(255), comment='是否参与滚动计算标识')
    difference_reason = Column(VARCHAR(255), comment='异常原因')
    difference_remark = Column(VARCHAR(255), comment='异常备注')
    difference_handle_date = Column(DateTime, comment='异常账单处理日期')
    handreflag = Column(VARCHAR(255), comment='0-未勾稽1-已勾稽2-已生成差异')
    differential_handling_id = Column(VARCHAR(255), comment='差异处理表id')
    credential_push_type = Column(VARCHAR(255), comment='推送凭证类型')
    historical_payment_status = Column(VARCHAR(255), comment='历史回款状态')
    is_generate_difference_processing = Column(VARCHAR(255), comment='是否生成差异处理单 1-是2-否')
    difference_processing_id = Column(VARCHAR(255), comment='差异处理单id')

    def create_table(self):
        Base.metadata.create_all(self)


class BillingReconciliation(Base):
    __tablename__ = 'billing_reconciliation'

    BillId = Column(VARCHAR(36), primary_key=True, comment='账单表ID')
    Bill_From = Column(VARCHAR(10), comment='账单来源')
    Business_Cate = Column(VARCHAR(10), comment='业务类型')
    Type = Column(VARCHAR(10), comment='收支类型')
    Match_State = Column(VARCHAR(10), comment='订单匹配状态')
    Collection_State = Column(VARCHAR(10), comment='回款状态')
    Bill_Tran_No = Column(VARCHAR(50), comment='账务流水号')
    Business_Basic_SOID = Column(VARCHAR(50), comment='业务基础订单号')
    Channel = Column(VARCHAR(50), comment='交易渠道')
    Datetime_Occur = Column(DateTime, comment='发生时间')
    Amount_In_Out = Column(DECIMAL(18, 3), comment='收入总金额')
    Datetime_Verify_Last = Column(DateTime, comment='上次对账日期')
    PlatformID = Column(VARCHAR(50), comment='平台代码')
    Platform_Name = Column(VARCHAR(50), comment='平台名称')
    ShopID = Column(VARCHAR(50), comment='门店代码')
    Shop_Name = Column(VARCHAR(50), comment='店铺名称')
    Bill_Cate = Column(VARCHAR(50), comment='账单类型')
    Diff_Reason = Column(VARCHAR(50), comment='异常原因')
    NET_SOID = Column(VARCHAR(200), comment='网店交易号')
    Diff_Remarks = Column(VARCHAR(2000), comment='异常备注')
    Diff_Date = Column(DateTime, comment='异常账单处理日期')
    HandReFlag = Column(VARCHAR(1), server_default=text("'0'"), comment='0-未勾稽1-已勾稽2-已生成差异')
    OrderGUID = Column(VARCHAR(36), comment='关联订单表id')
    ExcepId = Column(VARCHAR(36), comment='差异处理表id')
    His_Collection_State = Column(VARCHAR(10), comment='历史回款状态')
    Cert_Type = Column(VARCHAR(50), comment='推送凭证类型')
    Amount_Diff = Column(DECIMAL(18, 3), comment='差异金额')
    Is_Create = Column(VARCHAR(1), comment='是否生成差异处理单1-是2-否')
    DIffSheetId = Column(VARCHAR(36), comment='差异处理单id')

    def create_table(self):
        Base.metadata.create_all(self)


def table_exists(name):
    insp = inspect(engine)
    return insp.has_table(name)


def init_reconciliation_table():
    if not table_exists(OrderReconciliation.__tablename__):
        OrderReconciliation.create_table(engine)
    if not table_exists(BillingReconciliation.__tablename__):
        BillingReconciliation.create_table(engine)


def get_reconciliation_order_list(order_data):
    data_list = []
    for data in order_data:
        handled_data = processing_oder_data(data)
        data_list.append(handled_data)
    return data_list


def get_reconciliation_bill_list(bill_data):
    data_list = []
    for data in bill_data:
        handled_data = processing_billing_data(data)
        data_list.append(handled_data)
    return data_list


def processing_oder_data(data):
    orderReconciliation = OrderReconciliation()
    orderReconciliation.id = uuid.uuid4()
    orderReconciliation.platform_id = 'tmall'
    orderReconciliation.platform_name = '天猫'
    orderReconciliation.shop_id = 'xxxx'
    orderReconciliation.shop_name = '天猫'
    orderReconciliation.order_id = data.订单编号
    orderReconciliation.order_date = data.订单付款时间
    orderReconciliation.shipping_date = data.发货时间 if ((data.发货时间 != 'null') and (data.发货时间 != '')) else None
    orderReconciliation.order_amount = data.买家应付货款
    orderReconciliation.order_match_state = '正常'
    orderReconciliation.order_payment_status = '未匹配'
    orderReconciliation.order_category = '销售订单'
    orderReconciliation.products_number = data.宝贝总数量
    orderReconciliation.total_merchandise = data.总金额
    orderReconciliation.payment_matching = 0
    orderReconciliation.difference_amount = data.买家应付货款
    orderReconciliation.online_transaction_number = data.订单编号
    orderReconciliation.is_overdue = '否'
    orderReconciliation.is_finish = '是'
    return orderReconciliation


def processing_billing_data(data):
    billingReconciliation = BillingReconciliation()
    billingReconciliation.BillId = uuid.uuid4()
    billingReconciliation.Bill_From = '支付宝'
    billingReconciliation.Business_Cate = data.账务类型
    billingReconciliation.Type = judge_income_outcome(data.收入, data.支出)
    billingReconciliation.Match_State = '正常'
    billingReconciliation.Collection_State = '未匹配'
    billingReconciliation.Bill_Tran_No = data.支付宝流水号
    billingReconciliation.Business_Basic_SOID = data.业务基础订单号
    billingReconciliation.Channel = '支付宝'
    billingReconciliation.Datetime_Occur = data.入账时间
    billingReconciliation.Amount_In_Out = get_amount_total_revenue(data.收入, data.支出)
    billingReconciliation.PlatformID = 'tianmao'
    billingReconciliation.Platform_Name = '天猫'
    billingReconciliation.ShopID = 'xxxxxxx'
    billingReconciliation.Shop_Name = '天猫商城'
    billingReconciliation.Bill_Cate = get_bill_category(data.业务描述, data.备注)
    billingReconciliation.NET_SOID = data.业务基础订单号
    return billingReconciliation


def judge_income_outcome(income, outcome):
    if decimal.Decimal(income) > 0 and decimal.Decimal(outcome) == 0:
        return '收入'
    elif decimal.Decimal(income) == 0 and decimal.Decimal(outcome) > 0:
        return '支出'
    else:
        return


def get_amount_total_revenue(income, outcome):
    if income > 0 and outcome == 0:
        return income
    elif income == 0 and outcome > 0:
        return outcome
    else:
        return 0


def get_bill_category(business_description, remark):
    if TRANSACTION_COLLECTION in business_description:
        return TRANSACTION_COLLECTION
    elif TRANSACTION_COLLECTION in business_description and REFUND_DURING_SALE not in business_description:
        return TRANSACTION_REPAYMENT
    elif TRANSACTION_REPAYMENT in business_description and REFUND_DURING_SALE in business_description:
        return TRANSACTION_REFUND
    elif TRANSACTION_REFUND in business_description:
        return TRANSACTION_REFUND
    elif REFUND_DURING_SALE in remark and (business_description is None):
        return TRANSACTION_REFUND
    else:
        return ''


def get_connection_string(file_path):
    with open(file_path, 'r', encoding='utf-8') as config_file:
        json_config = json.loads(config_file.read())
        return f'mysql+pymysql://{json_config["userName"]}:{json_config["passWord"]}@{json_config["host"]}:{json_config["port"]}/{json_config["dataBase"]}'


def check_arg():
    args = sys.argv[1:]
    try:
        opts, args = getopt.getopt(args, "s:e:f:h",
                                   ["start_time=", "end_time=", "file_path=", "help"])
        for opt_name, opt_value in opts:
            if opt_name in ('-h', '--help'):
                command_help()
            elif opt_name in ('-s', '--start_time'):
                command_start_time(opt_value)
            elif opt_name in ('-e', '--end_time'):
                command_end_time(opt_value)
            elif opt_name in ('-f', '--file_path'):
                command_file_path(opt_value)
    except getopt.error as err:
        print(err)
        print("use -h/--help for command line help")
        sys.exit(2)


def command_help():
    print("""
    -s 开始时间 格式 2021-03-02
    -e 结束时间 格式 2021-03-03
    -h 显示命令行帮助
    -f 配置文件路径 格式 D:\Work\code\solution\dbConfig.json
    --connection= 连接字符串
    --start_time= 开始时间 格式 2021-03-02
    --end_time= 结束时间 格式 2021-03-03
    --file_path= 文件路径 格式 D:\Work\code\solution\dbConfig.json
    --help 显示命令行帮助
    """)
    sys.exit(2)


def command_file_path(arg_value):
    global FILE_PATH
    FILE_PATH = arg_value


def command_start_time(arg_value):
    global START_DATE
    START_DATE = datetime.datetime.strptime(arg_value, "%Y-%m-%d")


def command_end_time(arg_value):
    global END_DATE
    END_DATE = datetime.datetime.strptime(arg_value, "%Y-%m-%d")


if __name__ == '__main__':
    check_arg()
    connection_string = get_connection_string(FILE_PATH)
    engine = create_engine(connection_string)
    session = sessionmaker(engine)()
    init_reconciliation_table()
    orders = session.query(TMallOrder).distinct().filter(
        TMallOrder.发货时间.between(START_DATE, END_DATE)).all()
    reconciliation_order_list = get_reconciliation_order_list(orders)
    session.add_all(reconciliation_order_list)
    session.commit()
    bills = session.query(AlipayBill).distinct().all()
    reconciliation_billing_list = get_reconciliation_bill_list(bills)
    session.add_all(reconciliation_billing_list)
    session.commit()
