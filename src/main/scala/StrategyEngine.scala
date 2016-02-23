import org.nirvana._
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Socket}
import java.io._
import java.nio.Buffer
import java.nio.ByteOrder
import java.nio.ByteBuffer
import scala.math.abs
import org.joda.time.{Period, DateTime, Duration, LocalTime, Minutes}

object StrategyEngine {

  def calcMtmPnL(asOfDate: DateTime): List[(String, String, PnLCalcRow)] = {
    var return_list = List[(String, String, PnLCalcRow)]()

    val allSty = DBProcessor.getAllStyFromTradesTable
    val mapStySymTF = {
      (allSty.map(
        s => s -> (DBProcessor.getAllTradesForSty(s))
          .map(tf => (tf.symbol, tf))
          .groupBy(_._1)
          .map { case (k, v) => (k, v.map(_._2).filter(tf => asOfDate.getMillis >= tf.datetime.getMillis)) }
      )).toMap
    }

    for ((sty, mapslisttf) <- mapStySymTF) {
      for ((sym, listtf) <- mapslisttf) {

        var trdPxLast: Double = 0
        var cumSgndVolLast: Double = 0
        var cumSgndVol: Double = 0
        var cumSgndNotlAdjChgSgn: Double = 0
        var avgPx: Double = 0
        var cumTotPnL: Double = 0
        var psnClosed: Double = 0
        var avgPxLast: Double = 0
        var cumRlzdPnL: Double = 0

        val listpnlcalctbl = listtf.map { tf =>

          cumSgndVol += tf.signed_volume

          if (cumSgndVolLast * cumSgndVol < SUtil.EPSILON) {
            psnClosed = cumSgndVolLast
          }
          else {
            if (abs(cumSgndVol) < abs(cumSgndVolLast))
              psnClosed = cumSgndVolLast - cumSgndVol
            else
              psnClosed = 0
          }

          val rlzdPnL = psnClosed * (tf.trade_price - avgPxLast)
          cumRlzdPnL += rlzdPnL

          if (cumSgndVolLast * cumSgndVol < SUtil.EPSILON) {
            cumSgndNotlAdjChgSgn = tf.trade_price * cumSgndVol
            avgPx = tf.trade_price
          }
          else {
            cumSgndNotlAdjChgSgn += tf.signed_notional + rlzdPnL
            avgPx = cumSgndNotlAdjChgSgn / cumSgndVol
          }

          val prdTotPnL = cumSgndVolLast * (tf.trade_price - trdPxLast)
          cumTotPnL += prdTotPnL

          val cumUrlzdPnL = cumTotPnL - cumRlzdPnL

          val pnlcalcrow = PnLCalcRow(
            tf.trade_price,
            tf.trade_volume,
            tf.trade_sign,
            cumSgndVol,
            cumSgndNotlAdjChgSgn,
            avgPx,
            prdTotPnL,
            cumTotPnL,
            psnClosed,
            rlzdPnL,
            cumRlzdPnL,
            cumUrlzdPnL
          )

          trdPxLast = tf.trade_price
          cumSgndVolLast = cumSgndVol
          avgPxLast = avgPx

          pnlcalcrow
        }

        val latestNominalPx = DBProcessor.getNominalPricesAsAt(asOfDate, sym)
        listpnlcalctbl match {
          case Nil => Nil
          case _ =>
            {
              val pnlcalcrow_last = listpnlcalctbl.last

              //--------------------------------------------------
              // Do MTM if it has outstanding position
              //--------------------------------------------------
              val dbrowtoinsert = {
                if (abs(pnlcalcrow_last.cumSgndVol) > SUtil.EPSILON)
                  pnlcalcrow_last.copy(cumUrlzdPnL = pnlcalcrow_last.cumUrlzdPnL + (pnlcalcrow_last.cumSgndVol * (latestNominalPx.get(sym).getOrElse(pnlcalcrow_last.trdPx) - pnlcalcrow_last.trdPx)))
                else
                  pnlcalcrow_last
              }

              return_list ::= (sty, sym, dbrowtoinsert)
            }
        }
      }
    }
    return_list

  }

  def main(args: Array[String]) =
    {
      println("StrategyEngine starts...")
      if (args.length < 1) {
        println("Please provide the property file as the input argument")
        System.exit(1)
      }
      println("Using property file: " + args(0))
      Config.readPropFile(args(0))

      //--------------------------------------------------
      val thdMDHandler = new Thread(new Runnable {
        def run() {

          var map_pt = Map[String, PeriodicTask]() // symbol - PeriodicTask

          //  Prepare our context and socket
          val context = ZMQ.context(1)
          // val socket = context.socket(ZMQ.REP)
          val socket = context.socket(ZMQ.PULL)
          socket.bind(Config.zmqMDConnStr)

          while (true) {
            val request = socket.recv(0)

            val recvdStr = new String(request, 0, request.length)
            println("Received MD: [" + recvdStr + "]")
            //--------------------------------------------------
            // insert into database only at a certain time interval
            //--------------------------------------------------
            val (parseRes, mfnominal) = SUtil.parseMarketFeedNominal(recvdStr)
            if (parseRes) {
              if (!map_pt.contains(mfnominal.symbol)) {
                map_pt += mfnominal.symbol -> new PeriodicTask(Config.itrdMktDataUpdateIntvlInSec)
              }

              if (map_pt(mfnominal.symbol).checkIfItIsTimeToWakeUp(SUtil.getCurrentDateTime(HongKong()))) {
                DBProcessor.insertMarketDataToItrdTbl(recvdStr)
                // //--------------------------------------------------
                // // TODO not the real code for daily and hourly bars right now
                // //--------------------------------------------------
                // DBProcessor.insertMarketDataToHourlyTbl(recvdStr)
                // DBProcessor.insertMarketDataToDailyTbl(recvdStr)
              }
            }

            // //--------------------------------------------------
            // // send ack through zmq
            // //--------------------------------------------------
            // val reply = "OK ".getBytes
            // reply(reply.length - 1) = 0 //Sets the last byte of the reply to 0
            // socket.send(reply, 0)
            // //--------------------------------------------------
          }
          println("Thread thdMDHandler ends...")

        }
      })

      val thdTFHandler = new Thread(new Runnable {

        def run() {

          //  Prepare our context and socket
          val context = ZMQ.context(1)
          // val socket = context.socket(ZMQ.REP)
          val socket = context.socket(ZMQ.PULL)
          socket.bind(Config.zmqTFConnStr)

          while (true) {
            val request = socket.recv(0)

            val recvdStr = new String(request, 0, request.length)
            println("Received TF: [" + recvdStr + "]")
            DBProcessor.insertTradeFeedToDB(recvdStr)

            // //--------------------------------------------------
            // // send ack through zmq
            // //--------------------------------------------------
            // val reply = "OK ".getBytes
            // reply(reply.length - 1) = 0 //Sets the last byte of the reply to 0
            // socket.send(reply, 0)
            // //--------------------------------------------------
          }
          println("Thread thdTFHandler ends...")

        }
      })

      val thdPnLCalculator = new Thread(new Runnable {

        var pt = new PeriodicTask(Config.pnlCalcIntvlInSec)

        def run() {

          while (true) {
            if (pt.checkIfItIsTimeToWakeUp(SUtil.getCurrentDateTime(HongKong()))) {
              //--------------------------------------------------
              // continuously calculate the latest intraday PnL
              // then insert into PnL table and portfolio table
              //--------------------------------------------------
              calcMtmPnL(SUtil.getCurrentDateTime(HongKong())).foreach {
                case (x, y, z) => {
                  DBProcessor.insertPnLCalcRowToItrdPnLTbl(x, y, z)
                  DBProcessor.deletePortfolioTableWithStratID(x)
                  DBProcessor.insertPortfolioTbl(None, x, y, z.cumSgndVol, z.avgPx, z.cumUrlzdPnL)
                }
              }

              //--------------------------------------------------
              // check whether daily PnL should be updated
              // should update for all days up till today
              // then insert into PnL table and portfolio table
              //--------------------------------------------------
              val dt_last_daily_pnl = DBProcessor.getLastDateTimeInDailyPnLTbl
              val curLD = SUtil.getCurrentDateTime(HongKong()).toLocalDate()
              val curLT = SUtil.getCurrentDateTime(HongKong()).toLocalTime()
              val minFromMTM = Minutes.minutesBetween(curLT, Config.mtmTime).getMinutes()

              val lsDateTimesInRange = {
                val uptoLD = { if (minFromMTM > 0) curLD.minusDays(1) else curLD }

                if (dt_last_daily_pnl == None)
                  SUtil.getListOfDatesWithinRange(Config.ldStartCalcPnL, uptoLD, Config.mtmTime)
                else
                  SUtil.getListOfDatesWithinRange(dt_last_daily_pnl.get.plusDays(1).toLocalDate(), uptoLD, Config.mtmTime)
              }

              lsDateTimesInRange.foreach(
                dtInRng =>
                  calcMtmPnL(dtInRng).foreach {
                    case (x, y, z) => {
                      DBProcessor.insertPnLCalcRowToDailyPnLTbl(Some(dtInRng), x, y, z)
                      //--------------------------------------------------
                      // portfolios table only contains the latest snapshot, so no need to update here
                      //--------------------------------------------------
                    }
                  }
              )
            }

            Thread.sleep(10)
          }

        }
      })

      //--------------------------------------------------
      thdTFHandler.start
      thdMDHandler.start
      thdPnLCalculator.start

      while (true) {
        Thread.sleep(2000);
      }
      //--------------------------------------------------

      println("StrategyEngine ends...")
    }
}
