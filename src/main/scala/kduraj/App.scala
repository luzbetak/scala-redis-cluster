package kduraj

/**
  * App Class for querying Cassandra
  */
object App {

  var IP = "127.0.0.1"
  var filename = ""

  /**
    * Get command line parameters
    * @param args
    */
  private def get_command_line_parameters(args: Array[String]) = {

    if (args.size > 1) {
      println("args: " + args(0))
      IP = args(0).toString
      filename = args(1).toString
    }
  }

  /**
    * Main Method of App Class
    * @param args
    */
  def main(args: Array[String]) {

    get_command_line_parameters(args)
    val cluster = new RedisCluster()

    val redisKey = ("dev", "2017-01-03", "session_10")
    cluster.process_session(redisKey._1, redisKey._2, redisKey._3, 4000)
    cluster.display_redis_key_value(redisKey._1, redisKey._2, redisKey._3)

  }

  private def local = {
    val redis = new RedisSingle()
    redis.test_put_get()
    redis.flatFileInserts(filename)
    redis.getSessionId("C5EB7AE9-D7A0-4206-8C3A-12F061B76A9A")
  }

}
