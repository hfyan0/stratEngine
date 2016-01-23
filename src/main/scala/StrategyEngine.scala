import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Socket}
import java.io._
import java.nio.Buffer
import java.nio.ByteOrder
import java.nio.ByteBuffer
import scala.math.abs

object StrategyEngine {
  def calcPnLAndPutInDB {
    val allSty = DBProcessor.getAllStyFromTradesTable
    val mapStySymTF = (allSty.map(s => s -> (DBProcessor.getAllTradesForSty(s)).map(tf => (tf.symbol, tf)).groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) })).toMap

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

          if (cumSgndVolLast * cumSgndVol < Util.EPSILON) {
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

          if (cumSgndVolLast * cumSgndVol < Util.EPSILON) {
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

        val latestNominalPx = DBProcessor.getLatestNominalPrices
        val pnlcalcrow_last = listpnlcalctbl.last

        //--------------------------------------------------
        // Do MTM if it has outstanding position
        //--------------------------------------------------
        val dbrowtoinsert =
          if (abs(pnlcalcrow_last.cumSgndVol) > Util.EPSILON)
            pnlcalcrow_last.copy(cumUrlzdPnL = pnlcalcrow_last.cumUrlzdPnL + (pnlcalcrow_last.cumSgndVol * (latestNominalPx.get(sym).getOrElse(pnlcalcrow_last.trdPx) - pnlcalcrow_last.trdPx)))
          else
            pnlcalcrow_last

        DBProcessor.insertPnLCalcRowToDB(dbrowtoinsert, sty, sym)
      }
    }

  }
  def main(args: Array[String]) =
    {
      println("StrategyEngine starts...")

      //--------------------------------------------------
      val thdMDHandler = new Thread(new Runnable {
        def run() {

          //  Prepare our context and socket
          val context = ZMQ.context(1)
          val socket = context.socket(ZMQ.REP)
          socket.bind(Config.zmqMDConnStr)

          while (true) {
            val request = socket.recv(0)

            val recvdStr = new String(request, 0, request.length)
            println("Received MD: [" + recvdStr + "]")
            DBProcessor.insertMarketDataToDB(recvdStr)

            //--------------------------------------------------
            // send ack through zmq
            //--------------------------------------------------
            val reply = "OK ".getBytes
            reply(reply.length - 1) = 0 //Sets the last byte of the reply to 0
            socket.send(reply, 0)
            //--------------------------------------------------
          }
          println("Thread thdMDHandler ends...")

        }
      })

      val thdTFHandler = new Thread(new Runnable {
        def run() {

          //  Prepare our context and socket
          val context = ZMQ.context(1)
          val socket = context.socket(ZMQ.REP)
          socket.bind(Config.zmqTFConnStr)

          while (true) {
            val request = socket.recv(0)

            val recvdStr = new String(request, 0, request.length)
            println("Received TF: [" + recvdStr + "]")
            DBProcessor.insertTradeFeedToDB(recvdStr)

            //--------------------------------------------------
            // send ack through zmq
            //--------------------------------------------------
            val reply = "OK ".getBytes
            reply(reply.length - 1) = 0 //Sets the last byte of the reply to 0
            socket.send(reply, 0)
            //--------------------------------------------------
          }
          println("Thread thdTFHandler ends...")

        }
      })

      val thdPnLCalculator = new Thread(new Runnable {
        def run() {

          calcPnLAndPutInDB
          Thread.sleep(Config.pnlCalcIntvlInSec);
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
