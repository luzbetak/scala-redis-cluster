package kduraj

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import redis.clients.jedis.{HostAndPort, Jedis}

import scala.collection.JavaConversions.mapAsScalaMap

class RedisCluster {

  //val THRESHOLD: Int = 30 * 60 * 1000
  val THRESHOLD: Int = 100
  var counter = 0
  val node = new HostAndPort ("localhost",6379)
  //  val node = new HostAndPort ("192.168.0.120",6379)
  //  val cluster = new JedisCluster(node)
  val cluster = new Jedis("localhost");

  def uuid = java.util.UUID.randomUUID.toString

  /**
    * Add or subtract Date
    * @param date
    * @param days
    * @param inputFormat
    * @param outputFormat
    * @return
    */
  def dateAddDay(date: String, days: Int, inputFormat: String, outputFormat: String) : String = {
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    dateAux.add(Calendar.DATE, days)
    return new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }

  /**
    * Process session id
    * @param env
    * @param dt
    * @param session_id
    * @param current_timestamp
    */
  def process_session(env: String , dt: String, session_id: String, current_timestamp: Long): String = {

//    println(s"Processing: $env, $dt, $session_id, $current_timestamp")

    val session_info_today = get_session_time_delta(env, dt, session_id, current_timestamp)

    if(session_info_today._1.isEmpty) {
      println("Session Does not exist")
      val yesterday = dateAddDay(dt, -1, "yyyy-MM-dd", "yyyy-MM-dd")
      println(yesterday)

      val session_info_yesterday = get_session_time_delta(env, yesterday, session_id, current_timestamp)

      /*------------- Yesterday has No Session -------------*/
      if(session_info_yesterday._1.isEmpty) {
        val old_session_id = put_session_time(env, dt, session_id, session_id, current_timestamp)
        return old_session_id
      }
      /*------------- Yesterday has a Session --------------*/
      else {

        if(session_info_yesterday._3 > THRESHOLD) {
          val new_session_id = put_new_session_time(env, dt, session_id, current_timestamp)
          return new_session_id
        } else {
          val old_session_id = put_session_time(env, dt, session_id, session_info_yesterday._2, current_timestamp)
          return old_session_id
        }
      }

    }
    /*------------------ Today has Session ----------------*/
    else if(session_info_today._3 > THRESHOLD) {
      println("ABOVE: delta = " + session_info_today._3 )
      val new_session_id = put_new_session_time(env, dt, session_id, current_timestamp)
      return new_session_id
    } else {
      println("BELOW: delta = " + session_info_today._3 )
      val old_session_id = put_session_time(env, dt, session_id, session_info_today._2, current_timestamp)
      return old_session_id
    }

  }


  /**
    * Put time and session to Redis key/value
    * @param env
    * @param dt
    * @param session_id
    * @param current_timestamp
    */
  def put_session_time(env: String , dt: String, session_id: String, sessionized_id: String, current_timestamp: Long): String = {
    val redis_key = env + ":s:" + dt + ":" + session_id
    val map1: util.HashMap[String, String] = new java.util.HashMap[String, String]
    map1.put(current_timestamp.toString, sessionized_id)
    cluster.hmset(redis_key, map1)
    cluster.expire(redis_key, 86400 * 90)
    return sessionized_id
  }

  /**
    * Create a new session id for delta larget than 30 minutes
    * @param env
    * @param dt
    * @param session_id
    * @param current_timestamp
    * @return
    */
  def put_new_session_time(env: String , dt: String, session_id: String, current_timestamp: Long): String = {
    val redis_key = env + ":s:" + dt + ":" + session_id
    val map2: util.HashMap[String, String] = new java.util.HashMap[String, String]
    map2.put(current_timestamp.toString, uuid)
    cluster.hmset(redis_key, map2)
    cluster.expire(redis_key, 86400 * 90)
    uuid
  }

  /**
    * Display all the Redis values for particular key.
    * This method is mostly used for debugging
    * @param env
    * @param dt
    * @param session_id
    */
  def display_redis_key_value(env: String , dt: String, session_id: String): Unit = {

    val redis_key = env + ":s:" + dt + ":" + session_id
    // println(cluster.hgetAll(redis_key))
    val hashMap = cluster.hgetAll(redis_key)
    //for ((k, v) <- java_map) printf("key: %s, value: %s\n", k, v)

    val scalaMap = mapAsScalaMap(hashMap)
    val sorted = scalaMap.toSeq.sortBy(_._1.toLong)
    sorted.foreach(println)
    //println(java_map.toString)
    //println(java_map.get("3000"))
  }


  /**
    * Returns time delta between prior session
    * @param env
    * @param dt
    * @param session_id
    * @param current_timestamp
    * @return
    */
  def get_session_time_delta(env: String, dt: String, session_id: String, current_timestamp: Long): (String, String, Long) = {

    var difference = 0.0
    counter += 1
    val redis_key = env + ":s:" + dt + ":" + session_id
    println(s"redis_key = $redis_key")

    val redisAllFieldsValues = cluster.hgetAll(redis_key)

    if (redisAllFieldsValues == null || redisAllFieldsValues.isEmpty) {
      println("redis_value == null")
      val tuple = ("", "", "0".toLong)
      return tuple

    } else {

      redisAllFieldsValues.put(current_timestamp.toString, session_id)
      import scala.collection.JavaConversions.mapAsScalaMap
      //DEBUG: for ((k, v) <- java_map) printf("key: %s, value: %s\n", k, v)

      val redisScalaMap = mapAsScalaMap(redisAllFieldsValues)
      val sorted = redisScalaMap.toSeq.sortBy(_._1.toLong).reverse

      var FLAG = 0

      for ((k, v) <- sorted) {

        if ((k == current_timestamp.toString) && (FLAG == 0)) {
          //DEBUG: println(s"$k -> $v")
          FLAG = 1
        }
        else if (FLAG == 1) {
          //DEBUG: println(s"Previous session_id = $k -> $v")
          difference = current_timestamp - k.toLong
          val tuple = (k, v, difference.toLong)
          return tuple
        }

      }
      val tuple = ("", "", "0".toLong)
      return tuple
    }

  }




  /**
    * HMSET - appends existing HashMap in Redis Key/Value with new HashMap
    * Returns: { hash_key_4=hash_value_4, hash_key_3=hash_value_3, hash_key_2=hash_value_2,
    * hash_key_1=hash_value_1, hash_key_6=hash_value_6, hash_key_5=hash_value_5 }
    */
  def insert_hmset(env: String , dt: String, session_id: String, current_timestamp: Long): Unit = {

    val redis_key = env + ":s:" + dt + ":" + session_id
    println(s"redisKey = $redis_key")

    val map1: util.HashMap[String, String] = new java.util.HashMap[String, String]
    map1.put("1000", "session_1")
    map1.put("9000", "session_2")
    map1.put("8000", "session_3")
    cluster.hmset(redis_key, map1)
    cluster.expire(redis_key, 86400 * 90)

    val map2: util.HashMap[String, String] = new java.util.HashMap[String, String]
    map2.put("5000", "session_4")
    map2.put("6000", "session_5")
    map2.put("3000", "session_6")
    cluster.hmset(redis_key, map2)
    cluster.expire(redis_key, 86400 * 90)

    println(cluster.hgetAll(redis_key))
    println(cluster.hgetAll(redis_key).get("hash_key_5"))


    val res = cluster.hgetAll(redis_key)
    println("HGETALL = " + res.get("hash_key_3"))

    // get a single value
    println("HMSET = " + cluster.hmget(redis_key, "hash_key_1"))
    println("HMSET = " + cluster.hmget(redis_key, "hash_key_3"))
    println("HMSET = " + cluster.hmget(redis_key, "hash_key_4"))
    println("HMSET = " + cluster.hmget(redis_key, "hash_key_6"))

  }


}
