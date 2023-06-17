- 测试数据 
```json
{"user_id":1,"event_time":1686624203000,"event_id":"a","properties":{}}
{"user_id":1,"event_time":1686624204000,"event_id":"x","properties":{}}
{"user_id":2,"event_time":1686624204100,"event_id":"b","properties":{}}
{"user_id":2,"event_time":1686624206000,"event_id":"c","properties":{"p1":"v2"}}
{"user_id":3,"event_time":1686624206100,"event_id":"x","properties":{}}
{"user_id":3,"event_time":1686624206000,"event_id":"x","properties":{}}
{"user_id":4,"event_time":1686624207100,"event_id":"c","properties":{"p1":"v1"}}
{"user_id":4,"event_time":1686624207000,"event_id":"d","properties":{}}


{"user_id":8,"event_time":1686624207000,"event_id":"k","properties":{}}
{"user_id":8,"event_time":1686624207000,"event_id":"k","properties":{}}
{"user_id":8,"event_time":1686624207000,"event_id":"k","properties":{}}
{"user_id":8,"event_time":1686624207000,"event_id":"k","properties":{}}
{"user_id":8,"event_time":1686624207000,"event_id":"m","properties":{"p1":"v2"}}


{"user_id":1,"event_time":1686902464000,"event_id":"w","properties":{}}
{"user_id":1,"event_time":1686902465000,"event_id":"q","properties":{}}


{"user_id":1,"event_time":1686624207000,"event_id":"w","properties":{}}
{"user_id":1,"event_time":1686624207000,"event_id":"x","properties":{"p1":"v1"}}


{"user_id":3,"event_time":1686624207000,"event_id":"w","properties":{}}
{"user_id":3,"event_time":1686624207000,"event_id":"x","properties":{"p1":"v1"}}
{"user_id":3,"event_time":1686624207000,"event_id":"w","properties":{}}
{"user_id":3,"event_time":1686624207000,"event_id":"w","properties":{}}
{"user_id":3,"event_time":1686624207000,"event_id":"x","properties":{"p1":"v1"}}
```

- 致本项目的老读者 

“易学在线”官网发布的视频中，对于 “跨规则上线时间段” 动态画像统计条件的处理中，有一个预计算 条件start - currentTime的时间分割预计算方案，而历史区段结束时间点，到规则正式就绪于运算机池，之间有一段时间差，这段时间差可能导致少量数据的漏计算；在本版本的代码中，已经提供了解决方案，可在主架构代码的processElement中看到