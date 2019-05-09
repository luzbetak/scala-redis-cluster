package kduraj

class TestCase1 {

  //cluster.insert_hmset("dev", "2017-09-06", "session_6", 5500)
  //cluster.put_new_session_time("dev", "2017-09-06", "session_6", 3734)
  //println("******************")
  //println("******************")
  //cluster.insert_from_flatfile(filename)
  //cluster.test_cluster_connection()


  //val redis_value = result.get("3000")
  //if (redis_value == null || redis_value.isEmpty) {
  //  println("redis_value == null")
  //} else {
  //  println(redis_value.toString)
  //}

  //    if (redis_value == null || redis_value.isEmpty) {
  //
  //      //cluster.set(redis_key, current_timestamp + "|" + session_id)
  //      val map1: util.HashMap[String, String] = new java.util.HashMap[String, String]
  //      map1.put(current_timestamp.toString, session_id)
  //      cluster.hmset(redis_key, map1)
  //      cluster.expire(redis_key, 86400 * 90)
  //      return session_id
  //
  //    } else { // SessionId is found.
  //
  //      val old_timestamp = redis_value.split("\\|")(0).toLong
  //      val old_session_id = redis_value.split("\\|")(1)
  //
  //      // Difference between events is less than 30 minutes, then store the session_id,
  //      // storedSessionId (It may have already changed.) and currentTS
  //      if ((current_timestamp - old_timestamp) < (30 * 60 * 1000)) {
  //
  //        //cluster.set(redis_key, current_timestamp + "|" + old_session_id)
  //        val map2: util.HashMap[String, String] = new java.util.HashMap[String, String]
  //        map2.put(current_timestamp.toString, old_session_id)
  //        cluster.hmset(redis_key, map2)
  //        cluster.expire(redis_key, 86400 * 90)
  //        return old_session_id
  //
  //      }
  //
  //      // Difference between events is more than 30 minutes, then create new session_id from uuid and store the new uuid.
  //      else {
  //
  //        //cluster.set(redis_key, current_timestamp + "|" + uuid)
  //        val map3: util.HashMap[String, String] = new java.util.HashMap[String, String]
  //        map3.put(current_timestamp.toString, uuid)
  //        cluster.hmset(redis_key, map3)
  //        cluster.expire(redis_key, 86400 * 90)
  //        return uuid
  //
  //      }
  //
  //    }

  //  }

}
