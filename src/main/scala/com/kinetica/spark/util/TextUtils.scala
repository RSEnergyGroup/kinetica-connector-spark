package com.kinetica.spark.util

object TextUtils {

    def truncateToSize( doTruncate : Boolean, value: String, N : Integer ) : String = {
      if (doTruncate && (value.length > N)) {
        value.substring(0, N)
      } else {
        value // untruncated value
      }
    }
    
}
