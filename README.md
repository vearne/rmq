# rmq
The message queue based on redis is guaranteed to be successfully consumed at least once.

### Notice
1. This library is only for Redis version < 5.0.0 . For Redis(version > 5.0.0), Please use [stream](https://redis.io/topics/streams-intro).
2. The Message may be consumed more than once.