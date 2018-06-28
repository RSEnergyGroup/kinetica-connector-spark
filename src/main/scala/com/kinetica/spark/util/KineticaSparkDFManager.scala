package com.kinetica.spark.util

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.util.Iterator

import scala.beans.BeanProperty
import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gpudb.BulkInserter
import com.gpudb.GPUdb
import com.gpudb.GPUdbBase
import com.gpudb.GPUdbException
import com.gpudb.GenericRecord
import com.gpudb.Type
import com.kinetica.spark.LoaderParams
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._

object KineticaSparkDFManager extends LazyLogging {

    @BeanProperty
    var df: DataFrame = null

    var myType: Type = null

    /**
     * Methods returns kinetica table type
     * If table type is not set, method will call setType
     * @param lp Loader params
     * @return kinetica table type
     */
    def getType(lp: LoaderParams): Type = {
        if (myType == null) {
            setType(lp)
        }
        myType
    }

    def setType(lp: LoaderParams): Unit = {
        try {
            logger.debug("Kinetica URL is " + lp.getKineticaURL)
            val gpudb: GPUdb = lp.getGpudb
            logger.debug(" Attempting Type.fromTable for table name " + lp.getTablename)
            myType = Type.fromTable(gpudb, lp.getTablename)
        } catch {
            case e: GPUdbException => e.printStackTrace()
        }
    }

    /**
     * Returns Kinetica table type
     * Use with care.  Does not call set type method if type is not set
     * @return Kinetica table type
     */
    def getType(): Type = myType

    def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }

    /**
     * Maps over dataframe using either matching columns or chronological ordering
     * @param lp LoaderParams
     */
    def KineticaMapWriter(sc: SparkContext, lp: LoaderParams): Unit = {

        logger.debug("KineticaMapWriter")
        val typef: Type = myType
        val bkp: LoaderParams = lp
        logger.debug("Mapping Dataset columns to Kinetica")
        df.foreachPartition(new ForeachPartitionFunction[Row]() {
            def call(t: Iterator[Row]): Unit = {
                val kbl: KineticaBulkLoader = new KineticaBulkLoader(bkp)
                val bi: BulkInserter[GenericRecord] = kbl.GetBulkInserter()
                while (t.hasNext) {
                    
                    lp.totalRows.add(1)
                    val row: Row = t.next()
                    val genericRecord: GenericRecord = new GenericRecord(typef)
                    var i: Int = 0
                    for (column <- typef.getColumns) {
                        try {
                            var rtemp: Any = row.get({ i += 1; i - 1 })
                            if (lp.isMapToSchema) {
                                rtemp = row.getAs(column.getName)
                            }
                            if (!putInGenericRecord(genericRecord, rtemp, column)) {
                                lp.failedConversion.add(1)
                            }
                        } catch {
                            case e: Exception =>
                                //e.printStackTrace()
                                lp.failedConversion.add(1)
                                logger.warn("Found non-matching column DS.column --> KineticaTable.column, moving on", e)
                                throw e
                        }
                    }
                    lp.convertedRows.add(1)
                    bi.insert(genericRecord)
                }
                try bi.flush()
                catch {
                    case e: Exception => {
                        logger.error("Flush error", e)
                        e.printStackTrace()
                    }
                }
            }
        })
    }
    
    def putInGenericRecord(genericRecord : GenericRecord, rtemp : Any, column : Type.Column ) : Boolean = {
        
        //println(" Adding 1 record 1 field .........")
        
        var isARecord: Boolean = false
        if (rtemp != null) {
        	logger.debug("Spark type {} not null", rtemp.getClass())
        	if (rtemp.isInstanceOf[java.lang.Long]) {
        		logger.debug("Long")
        		genericRecord.put(column.getName, rtemp)
        		isARecord = true
        	} else if (rtemp.isInstanceOf[Timestamp]) {
        		logger.debug("Timestamp instance")
        		genericRecord.put(column.getName, classOf[Timestamp].cast(rtemp).getTime)
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.sql.Date]) {
        		logger.debug("Date instance")
        		genericRecord.put(column.getName, rtemp.toString)
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Boolean]) {
        		logger.debug("Boolean instance")
        		if (classOf[Boolean].cast(rtemp).booleanValue()) {
        			logger.debug("Cast to 1")
        			genericRecord.put(column.getName, 1)
        			isARecord = true
        		} else {
        			logger.debug("Cast to 0")
        			genericRecord.put(column.getName, 0)
        			isARecord = true
        		}
        	} else if (rtemp.isInstanceOf[BigDecimal]) {
        		logger.debug("BigDecimal")
        		genericRecord.put(column.getName, classOf[BigDecimal].cast(rtemp).doubleValue())
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Short]) {
        		logger.debug("Short")
        		genericRecord.put(column.getName, classOf[Short].cast(rtemp).intValue())
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Float]) {
        		logger.debug("Float")
        		if (column.getType().toString().contains("java.lang.Float")) {
        		    genericRecord.put(column.getName, classOf[java.lang.Float].cast(rtemp).floatValue())
	        		isARecord = true
        		} else if (column.getType().toString().contains("java.lang.Double")) {
        		    genericRecord.put(column.getName, toDouble(rtemp))
	        		isARecord = true
        		} else {
        		   logger.debug("Kinetica column type is " + column.getType + " for name " + column.getName) 
        		}
        	} else if (rtemp.isInstanceOf[java.lang.Double]) {
        		logger.debug("Double")
        		genericRecord.put(column.getName, classOf[java.lang.Double].cast(rtemp).doubleValue())
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Byte]) {
        		logger.debug("Byte")
        		genericRecord.put(
        			column.getName,
        			classOf[Byte].cast(rtemp).intValue())
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.String]) {
        		logger.debug("String found, column type is " + column.getType + " for name " + column.getName)
        		// This is the path most travelled....
        		if (column.getType().toString().contains("java.lang.Double")) {
        			genericRecord.put(column.getName, rtemp.toString().toDouble)
        		} else if (column.getType().toString().contains("java.lang.Float")) {
        			genericRecord.put(column.getName, rtemp.toString().toFloat)
        		} else if (column.getType().toString().contains("java.lang.Integer")) {
        			genericRecord.put(column.getName, rtemp.toString().toInt)
        		} else if (column.getType().toString().contains("java.lang.Long")) {
        			genericRecord.put(column.getName, rtemp.toString().toLong)
        		} else {
        			genericRecord.put(column.getName, rtemp.toString())
        		}
        		isARecord = true
        	 } else {
        		logger.debug("Spark type {} ", rtemp.getClass())
        		logger.debug("Kin instance type is {} {}", column.getType, column.getName)
        		genericRecord.put(
        			column.getName,
        			column.getType.cast(rtemp))
        		isARecord = true
        	}
        }
        isARecord
    }
        
}
/*
if( maptoschem ) {
	Kinetica type t
	for each row r {
		for each col c in t {
			colValue = r.getAs(c.getName)
			gr.put(c.getName, cast(colValue))
		}
	}
} else {
	Kinetica type t
	for each row r {
		int cnt = 0;
		for each col c in t {
			colValue = r.get(cnt++)
			gr.put(c.getName, cast(colValue))
		}
	}			
}
*/
