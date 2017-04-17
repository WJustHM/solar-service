# REST-API 使用手册

**该手册仅对REST-API使用说明,并随着API的更新而同步更新.**  
**更多内容及格式要求陆续完善中.**  

*** 

4/17更新： 调整API排序，所有API添加Token认证，新增3个布控相关API  

***  

## API列表  

### 1. 登录认证  
* 请求方式:POST  
* 条件参数 : 用户名[username], 密码[password], 部门[department]  
* 示例:  
>http://datanode1:8001/solar/traffic/authenticate  
>POST-Body:{"username":"admin","password":"admin123","department":"traffic"}  

* 返回结果:认证成功  
```json
{
    "access": {
        "x-token": "YXxcQ4CiFYk5DJo1VsmCpR6vm1PcuH0ObiL2gAAh5uQVWd7mnPuS+NhfmCPeff37C5+IyKVB5Ht0e+rLKwlFpdm43a1AkWCys0649H2kuK0NUpic9kjpBDI81LyPFRdaSqZVwO/05YuV8RVQcQLopvgwoXhUhtFHJPvKRr8Dxl8=",
        "endpoint": "/solar/traffic"
    }
}
 ```
* 返回结果:认证失败
```json
{
    "Result": "0",
    "Error": "Failed to authentication"
}
```

### 2. 轨迹查询  
* 请求方式:GET  
* Header: 认证令牌[x-token]  
* 条件参数 : 开始时间[start], 结束时间[end], 车牌号[PlateLicense]  
* 示例:  
>http://datanode1:8001/traffic/track?start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&PlateLicense=川Z78038  

* 返回结果:text/json(只显示部分结果)  
```json
{
    "2017-03-29 11:47:28": {
        "SBBH": "42",
        "Vehicle_Speed": "20.0",
        "RowKey": "096425277401476_6",
        "lonlat": "104.067883,30.731568"
    },
    "2017-03-29 11:53:43": {
        "SBBH": "55",
        "Vehicle_Speed": "26.0",
        "RowKey": "112767518774476_0",
        "lonlat": "104.097204,30.725608"
    },
    "2017-03-29 12:00:37": {
        "SBBH": "54",
        "Vehicle_Speed": "29.0",
        "RowKey": "745534666398476_2",
        "lonlat": "104.139748,30.719399"
    },
    "......": {
        "......": "......"
    }
}
```

### 3. 图片查询  
* 请求方式:GET  
* Header: 认证令牌[x-token]  
* 需要提供 : HBase行键[rowkey]  
* 示例:  
>http://172.18.199.11:8001/solar/traffic/image/313668800076876_3  

 返回结果:
 image/png(此处不展示)
 
 
### 4. 流量统计查询  
* 请求方式:GET  
* Header: 认证令牌[x-token]  
* 条件参数 : 查询粒度[by], 开始时间[start], 结束时间[end], 卡口号[deviceId]  
* 示例:  
>http://datanode1:8001/solar/traffic/statistics?by=minute&start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=hour&start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=day&start="2017-03-29 11:00:00"&end="2017-03-30 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=month&start="2017-03-29 11:00:00"&end="2017-03-30 15:00:00"&deviceId=12  

* 返回结果:text/json(只显示按小时统计的部分结果)  
```json
{
    "statistics":
        {
            "0": 57237,
            "1": 37978,
            "2": 19278,
            "3": 18723,
            "total": 133216,
            "2017-03-29 11|2": 1396,
            "2017-03-29 11|1": 3015,
            "2017-03-29 11|0": 4364,
            "2017-03-29 11|3": 1372,
            "2017-03-29 12|1": 3015,
            "2017-03-29 12|3": 1388,
            "2017-03-29 12|0": 4470,
            "2017-03-29 12|2": 1532,
            "......": "......"
        }
}
```

### 5. 卡口查询  
* 请求方式:GET  
* Header: 认证令牌[x-token]  
* 条件参数 : 无  
* 示例:  
>http://datanode1:8001/solar/traffic/devices  

* 返回结果:text/json(只显示部分结果)  
```json
{
    "devices": [
        {
            "id": "1",
            "coordinate": {
                "latitude": "30.792887",
                "longitude": "104.054085"
            },
            "monitortype": "1"
        },
        {
            "id": "2",
            "coordinate": {
                "latitude": "30.787675",
                "longitude": "104.040862"
            },
            "monitortype": "1"
        },
        {
            "......": "......"
        }
    ]
}
```

### 6. 布控名单查询
* 请求方式:GET  
* Header: 认证令牌[x-token]  
* 条件参数 : 布控行为[BKXW]
* 示例:  
>http://datanode1:8001/solar/traffic/preyinfo?BKXW=03  

* 返回结果:text/json(只显示部分结果)  
```json
{
    "PreyList":
    [
        {
            "BKXXBH": "云B000272017-03-30 17:21:24",
            "HPHH": "云B00027",
            "HPYS": "1",
            "CLPP1": "2",
            "CLPP2": "2",
            "CSYS": "白",
            "CLLX": "2",
            "HPZL": "1"
        },
        {
            "BKXXBH": "云B100602017-03-29 17:20:38",
            "HPHH": "云B10060",
            "HPYS": "0",
            "CLPP1": "0",
            "CLPP2": "0",
            "CSYS": "金",
            "CLLX": "0",
            "HPZL": "4"
        }
    ]
}
```

### 7. 添加布控名单
* 请求方式:PUT  
* Header: 认证令牌[x-token]  
* 条件参数 : 布控信息  
* 示例:  
>http://172.18.199.11:8001/solar/traffic/preyinfo  
>PUT-Body:{"HPHM":"川L44044","HPYS":"3","CLPP1":"保时捷","CSYS":"黑","CLLX":"3","HPZL":"2","BKXW":"06"}  

* 返回结果：（此处后期会修改）  
成功：Add record success  
失败：Add failure  

### 8. 更新布控名单
注：要求原先的BKXXBH对应的记录必须存在，否则无法更新  

* 请求方式:POST  
* Header: 认证令牌[x-token]  
* 条件参数 : 布控信息编号[BKXXBH]  
* 示例:  
>http://172.18.199.11:8001/solar/traffic/preyinfo/川A222222017-04-14%2014:44:41  
>PUT-Body:{"HPYS":"3","CLPP1":"保时捷","CSYS":"银","CLLX":"3","HPZL":"2","BKXW":"02","BKJB":"1"}  

* 返回结果：（此处后期会修改）  
成功：Update record success  
失败：Update failure  








