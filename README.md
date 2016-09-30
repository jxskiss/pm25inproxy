# pm25inproxy

[pm25.in]()空气质量数据缓存代理服务。

[pm25.in]()网站为众多空气质量监测数据的使用者提供了难得的免费查询服务，首先向pm25.in
网站的组织者和维护者表达敬意。

## 简介

作为少数几个专业的空气质量数据提供网站之一，访问压力自然是比较大的，其提供的API服务都有
调用次数限制，在面对访问量比较大的应用时，一不小心就容易调用次数超限。pm25inproxy就是
为了解决这个问题而生的。该代理服务有三个特点：

1. 对限制次数少、定时更新的数据提供缓存，定时刷新缓存，访问代理服务的请求直接从缓存中取数据，
    不需要再次访问pm25.in网站，有效降低延时的同时严格控制了请求服务不会超出次数限制；
2. 被缓存的数据同时写入一份数据到硬盘，并提供历史数据查询服务，历史数据是压缩保存的，
    以节省磁盘空间，同时可通过命令行参数指定历史数据保存天数（pm25.in不提供任何形式的历史
    数据查询服务）；
3. 对限制次数相对较多，查询参数多的请求，直接代理到pm25.in，如果单个AppKey次数不够用，
    可以使用多个AppKey增加访问次数限制。

## API说明

本代理服务中设定的默认次数限制如下：

    # 1.1、获取一个城市所有监测点的PM2.5数据，代理转发
    'api/querys/pm2_5.json': 400,
    # 1.2、获取一个城市所有监测点的PM10数据，代理转发
    'api/querys/pm10.json': 400,
    # 1.3、获取一个城市所有监测点的CO数据，代理转发
    'api/querys/co.json': 400,
    # 1.4、获取一个城市所有监测点的NO2数据，代理转发
    'api/querys/no2.json': 400,
    # 1.5、获取一个城市所有监测点的SO2数据，代理转发
    'api/querys/so2.json': 400,
    # 1.6、获取一个城市所有监测点的O3数据，代理转发
    'api/querys/o3.json': 400,
    # 1.7、获取一个城市所有监测点的AQI数据（含详情），代理转发
    'api/querys/aqi_details.json': 400,
    # 1.8、获取一个城市所有监测点的AQI数据（不含详情，仅AQI），代理转发
    'api/querys/only_aqi.json': 400,
    # 1.9、获取一个监测点的AQI数据（含详情），代理转发
    'api/querys/aqis_by_station.json': 400,
    # 1.10、获取一个城市的监测点列表，被动缓存、触发更新
    'api/querys/station_names.json': 1,
    # 1.11、获取实施了新《环境空气质量标准》的城市列表，被动缓存、触发更新
    'api/querys.json': 1,
    # 1.12、获取所有城市的空气质量详细数据，主动缓存、定时更新
    'api/querys/all_cities.json': 3,
    # 1.13、获取全部城市的空气质量指数(AQI)排行榜，主动缓存、定时更新
    'api/querys/aqi_ranking.json': 3

关于pm25.in提供的API的使用文档请访问[pm25.in]()网站查看。

本代理服务启动时，请在命令行参数指定代理服务使用的AppKey，下游请求访问代理服务时候可选传递AppKey，
如果传递AppKey则使用请求传入的AppKey访问pm25.in，如果没有传入AppKey，则使用默认的AppKey访问
pm25.in。不同的AppKey请求次数限制单独计数。

### 特殊API

除了提供到pm25.in的API的代理访问外，该代理服务提供额外的历史数据查询服务和缓存状态查询服务。

* 历史数据查询

  在原本API请求后面增加`history=Ymdh`参数，如
  `/api/querys/all_cities.json?history=2016092812`。

* 缓存状态查询

  - `/status/`: 服务状态（"ok"）；
  - `/status/API`: 缓存数据时间（"2016-09-28T12:00:00"），如
    `/status/api/querys/all_cities.json`。

## 版本说明

Python版本与Golang版本（学习作品）支持相同的功能，提供完全相同的访问接口，如果两个程序表现不一样，
欢迎提交[issue](https://github.com/jxskiss/pm25inproxy/issues)。

Python版本使用Tornado框架提供异步请求支持，Golang版本天然goroutine也是异步的，所以理论上
两个版本都可以支持相当数量的并发请求。

注意：两个程序实现历史数据查询功能使用的数据库不同，并且格式互不兼容，因此两个程序无法相互共享历史数据。

## 使用示例

```bash
python pm25in.py --app_key=yourkey --store_dir=/mnt/data/pm25in
go run pm25in.go --pm25key=yourkey --datadir=/mnt/data/pm25in
```

更多选项请使用使用命令行参数`--help`查看。

## 反馈

欢迎任何形式的pr和issues。


[pm25.in]: http://www.pm25.in/
