package com.example.func

class Bloom(cap: Long) extends Serializable {

    def hash(value: String, seed: Int): Long = {
        var result = 0;
        for (i <- 0 until(value.length)){
            result = result * seed + value.charAt(i)
        }
        (cap - 1) & result
    }
}
