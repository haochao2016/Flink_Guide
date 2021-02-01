object CacuValue {
    def main(args: Array[String]): Unit = {


        println(hash("array", 23))
        println('y' + 1)
//        println(1<<30)
    }

    val cap : Long = 2^30

    // hash函数
    def hash(value: String, seed: Int): Long = {
        var result = 0
        for( i <- 0 until value.length ){
            result = result * seed + value.charAt(i)
        }
        // 返回hash值，要映射到cap范围内
        (cap - 1) & result
    }

}
