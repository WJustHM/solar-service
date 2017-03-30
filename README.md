# REST-API 使用手册

**该手册仅对REST-API使用说明,并随着API的更新而同步更新**
**更多内容及格要求陆续完善**

## API列表

* 轨迹查询
请求方式:GET  
需要提供 : 开始时间[start], 结束时间[end], 车牌号[PlateLicense]  
示例:  
>http://datanode1:8001/traffic/track?start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&PlateLicense=川Z78038  

* 图片查询
请求方式:GET  
需要提供 : HBase行键[rowkey]  
示例:  
>http://datanode1:8001/traffic/image?rowkey=875542703454876_8  

* 流量统计查询
请求方式:GET  
需要提供 : 查询粒度[by], 开始时间[start], 结束时间[end], 卡口号[deviceId]  
示例:  
>http://datanode1:8001/solar/traffic/statistics?by=minute&start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=hour&start="2017-03-29 11:00:00"&end="2017-03-29 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=day&start="2017-03-29 11:00:00"&end="2017-03-30 15:00:00"&deviceId=12  
>http://datanode1:8001/solar/traffic/statistics?by=month&start="2017-03-29 11:00:00"&end="2017-03-30 15:00:00"&deviceId=12  

* 卡口查询
请求方式:GET  
需要提供 : 无  
示例:  
>http://datanode1:8001/solar/traffic/devices  

* 登录认证
请求方式:POST  
需要提供 : 用户名[id], 密码[password], 部分[department]  
示例:  
>http://datanode1:8001/solar/traffic/authenticate  
>POST-body:{"id":"admin","password":"admin123","department":"traffic"}  

