# 模块划分

* ./pkg/source

实现api.MessageHandler接口，从一定的数据源读取数据，可以后接api.MessageHandler

source -> api.MessageHandler

* ./pkg/sink

实现api.MessageHandler接口，前接api.MessageHandler，将数据发往指定的数据源

api.MessageHandler -> sink

* ./pkg/stage

实现api.MesssageHandler接口，可以前后接api.MessageHandler

api.MessageHandler -> stage -> api.MessageHandler

## 常见组合模式

source -> stage_1 -> stage_2 -> sink