import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context,Socket}
import java.io._
import java.nio.Buffer
import java.nio.ByteOrder
import java.nio.ByteBuffer


object StrategyEngine
{
  val zmqConnStr = "tcp://*:5555"

    def main(args: Array[String]) =
    {
      println("StrategyEngine starts...")

        //  Prepare our context and socket
        val context = ZMQ.context(1)
        val socket = context.socket(ZMQ.REP)
        socket.bind (zmqConnStr)

        while (true) {
          val request = socket.recv (0)

            val recvdStr = new String(request,0,request.length)
            println ("Received string: [" + recvdStr + "]")
            DBProcessor.updateTradeFeedToDB(recvdStr)

            //--------------------------------------------------
            // reply zmq
            //--------------------------------------------------
            val reply = "OK ".getBytes
            reply(reply.length-1)=0 //Sets the last byte of the reply to 0
            socket.send(reply, 0)
        }
      println("StrategyEngine ends...")
    }
}
