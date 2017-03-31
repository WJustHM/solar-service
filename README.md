# REST-API 使用手册

**该手册仅对REST-API使用说明,并随着API的更新而同步更新**  
**更多内容及格要求陆续完善**

## API列表  

###轨迹查询  
* 请求方式:GET  
* 条件参数 : 开始时间[start], 结束时间[end], 车牌号[PlateLicense]  
* 示例:  
>http://datanode1:8001/traffic/track?start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&PlateLicense=川Z78038  

* 返回结果:text/json(以部分结果为例)  
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
    ......
}
```

###图片查询  
* 请求方式:GET  
* 需要提供 : HBase行键[rowkey]  
* 示例:  
>http://172.18.199.11:8001/solar/traffic/image/313668800076876_3  

 返回结果:image(此处不展示)
 
###流量统计查询  
* 请求方式:GET  
* 条件参数 : 查询粒度[by], 开始时间[start], 结束时间[end], 卡口号[deviceId]  
* 示例:  
>http://datanode1:8001/solar/traffic/statistics?by=minute&start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=hour&start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=day&start="2017-03-29 11:00:00"&end="2017-03-30 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=month&start="2017-03-29 11:00:00"&end="2017-03-30 15:00:00"&deviceId=12  

* 返回结果:text/json(以按小时统计为例)  
```json
{
	"total": 133216,
    "2": 19278,
    "1": 37978,
    "0": 57237,
    "3": 18723
}{
    "2017-03-29 11|2": 1396,
    "2017-03-29 11|1": 3015,
    "2017-03-29 11|0": 4364,
    "2017-03-29 11|3": 1372,
    "2017-03-29 12|1": 3015,
    "2017-03-29 12|3": 1388,
    "2017-03-29 12|0": 4470,
    "2017-03-29 12|2": 1532,
    ......
}
```

###卡口查询  
* 请求方式:GET  
* 条件参数 : 无  
* 示例:  
>http://datanode1:8001/solar/traffic/devices  

* 返回结果:text/json(以部分结果为例)  
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
        ......
    ]
}
```

###登录认证  
* 请求方式:POST  
* 条件参数 : 用户名[username], 密码[password], 部门[department]  
* 示例:  
>http://datanode1:8001/solar/traffic/authenticate  
>POST-body:{"username":"admin","password":"admin123","department":"traffic"}  

* 返回结果:认证成功  
```json
{
	"access":
        {
			"token": "token-XXX",
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



