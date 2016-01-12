object Util
{
  def getCurrentTimeStamp(): java.sql.Timestamp =
  {
    val today = new java.util.Date()
      new java.sql.Timestamp(today.getTime())
  }
  def getCurrentTimeStampStr(): String =
  {
    getCurrentTimeStamp.toString
  }

  //--------------------------------------------------
  // from 20160112_123456_000000 to 2016-01-12 12:34:56.000000
  //--------------------------------------------------
  def convertTimestampFmt1(ts: String) =
  {
    val tsFields = ts.split("_")
      if (tsFields.length != 3) ""
      else
      {
        val sYYYYMMDD = tsFields(0)
          val sHHMMSS = tsFields(1)
          val sMillisec = tsFields(2)

          val sYear = sYYYYMMDD.substring(0,4)
          val sMonth = sYYYYMMDD.substring(4,6)
          val sDay = sYYYYMMDD.substring(6,8)

          val sHour = sHHMMSS.substring(0,2)
          val sMin = sHHMMSS.substring(2,4)
          val sSec = sHHMMSS.substring(4,6)

          sYear + "-" + sMonth + "-" + sDay + " " + sHour + ":" + sMin + ":" + sSec + "." + sMillisec
      }
  }

}
