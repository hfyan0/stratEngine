object Config {
  //--------------------------------------------------
  // 
  //--------------------------------------------------
  val pnlCalcIntvlInSec = 300

  //--------------------------------------------------
  // JDBC
  //--------------------------------------------------
  // val jdbcConnStr = "jdbc:mysql://localhost/testingdatabase";
  val jdbcConnStr = "jdbc:mysql://47.89.30.25/algo_cfsg_dev";
  val jdbcUser = "algo_sa_dev"
  val jdbcPwd = "algo_sa_dev!QAZ"

  //--------------------------------------------------
  // zmq
  //--------------------------------------------------
  val zmqMDConnStr = "tcp://*:18044"
  val zmqTFConnStr = "tcp://*:18045"

}
