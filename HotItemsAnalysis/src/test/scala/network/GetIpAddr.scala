package network

import java.net.InetAddress

object GetIpAddr {
    def main(args: Array[String]): Unit = {
//        java.net.InetAddress.getCanonicalHostName()
        val host = InetAddress.getLocalHost.getHostAddress
        println(InetAddress.getLocalHost.getHostName)
        println(InetAddress.getLocalHost.getCanonicalHostName)
        println(host)


    }

}
