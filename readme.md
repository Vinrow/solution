### 安装教程

1、使用python包管理工具安装依赖

```
pip install -r requirements.txt
```

2、修改数据库配置文件

修改dbConfig.json文件



### 使用说明

数据库表名称

| order_tmall | 天猫订单表   |
| ----------- | ------------ |
| bill_alipay | 支付宝账单表 |





运行python命令

```python
python main.py -s 1900-01-20 -e 1900-01-21 -f D:\Work\code\solution\dbConfig.json
```



### 其他

如需修改命令行参数，参照源文件的check_arg()方法