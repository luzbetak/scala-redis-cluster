package kduraj

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util
import redis.clients.jedis.Jedis
import scala.io.Source

/**
  * Redis Database
  */
class RedisSingle() {

  val jedis = new Jedis("localhost");

  def test_put_get(): Unit = {

    jedis.set("key", "value");
    val value = jedis.get("key");
    println(value)

  }

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  /**
    * Get Hash
    * @param env
    * @param dt
    * @param session_id
    */
  def get_hmset(env: String="dev", dt: String="2017-09-06", session_id: String = "123456"): Unit = {
    val redis_key = env + ":" + dt + ":" + session_id
    println(s"redisKey = $redis_key")
    println(jedis.hgetAll(redis_key))

  }


  /**
    * HMSET - appends existing HashMap in Redis Key/Value with new HashMap
    * Returns: { hash_key_4=hash_value_4, hash_key_3=hash_value_3, hash_key_2=hash_value_2,
    * hash_key_1=hash_value_1, hash_key_6=hash_value_6, hash_key_5=hash_value_5 }
    */
  def insert_hmset(env: String="dev", dt: String="2017-09-06", session_id: String = "123456"): Unit = {

    val redis_key = env + ":" + dt + ":" + session_id
    println(s"redisKey = $redis_key")

    val map1: util.HashMap[String, String] = new java.util.HashMap[String, String]
    map1.put("hash_key_1", "value_1")
    map1.put("hash_key_2", "value_2")
    map1.put("hash_key_3", "value_3")
    jedis.hmset(redis_key, map1)
    jedis.expire(redis_key, 86400)

    val map2: util.HashMap[String, String] = new java.util.HashMap[String, String]
    map2.put("hash_key_4", "value_4")
    map2.put("hash_key_5", "value_5")
    map2.put("hash_key_6", "value_6")
    jedis.hmset(redis_key, map2)
    jedis.expire(redis_key, 86400)

    println(jedis.hgetAll(redis_key))
    println(jedis.hgetAll(redis_key).get("hash_key_5"))


    val res = jedis.hgetAll(redis_key)
    println("HGETALL = " + res.get("hash_key_3"))

    // get a single value
    println("HMSET = " + jedis.hmget(redis_key, "hash_key_1"))
    println("HMSET = " + jedis.hmget(redis_key, "hash_key_3"))
    println("HMSET = " + jedis.hmget(redis_key, "hash_key_4"))
    println("HMSET = " + jedis.hmget(redis_key, "hash_key_6"))

  }

  /**
    * HGETALL
    */
  def get_non_existed_key(): Unit = {

    println(jedis.hexists("redis_key","hash_key_4"))
    // println(jedis.hlen("key").getClass)

    if(jedis.hlen("dev:key") > 0) {
      val res = jedis.hgetAll("redis_key")
      println("HGETALL = " + res)
    } else {
      println("Is Empty")
    }

  }

  /**
    * Get non-existing key with try catch
    */
  def get_non_existed_key_with_exception(): Unit = {

    try {
      val result3 = jedis.hgetAll("key")
      println("HGETALL = " + result3)
    } catch {
      case _: Throwable => {
        println("Key Does not exist")
      }
    }

  }

  /**
    * HSET returns null when non-existent key is accessed
    */
  def insert_hset(): Unit = {

    jedis.hset("dev:key1", "first", "Kevin")
    jedis.hset("dev:key1", "last", "Duraj")

    val result1 = jedis.hget("dev:key1", "last")
    println(result1)

    // Try to get key that does not exist
    val result2 = jedis.hget("dev:key2", "last")
    println(result2)

  }

  /**
    * Get Session Id
    *
    * @param session_id
    */
  def getSessionId(session_id: String): Unit = {

    val value = jedis.get(session_id);
    println(value)

  }

  /**
    * Insert data from a flat file
    * @param filename
    */
  def flatFileInserts(filename: String): Unit = {

    println(s"Reading Filename = $filename")

    //val buffer = io.Source.fromFile("data/links.dat")

    for (line <- Source.fromFile(filename).getLines) {

      val col = line.split("\\,").map(_.trim)
      //println(s"${col(0)}|${col(1)}")

      if (col.length == 2) {

        if ((col(0).length() > 2) && (col(1).length() > 2)) {
          println(col(0) + " ... " + col(1))
          jedis.set(col(0), col(1))

        } else {
          println(s"Column is too short = $line")
        }
      }
    }
  }

}
