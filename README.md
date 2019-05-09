Scala Redis Cluster
===================


### Redis Installation
```
$ curl -O http://download.redis.io/releases/redis-4.0.9.tar.gz
$ tar xzvf redis-4.0.9.tar.gz
$ cd redis-4.0.9
$ make
$ make install 

$ src/redis-server
$ src/redis-cli 
```

### Compile and Run Application
```
./build.sh && ./run.sh
```

### References:

* https://redis.io
* https://github.com/xetorthio/jedis
* https://mvnrepository.com/artifact/redis.clients/jedis
* https://github.com/redislabs/spark-redis

