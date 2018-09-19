package com.kinetica.spark.util

import com.gpudb._
import java.net.URL
import java.util.HashMap
import java.util.Iterator
import java.util.Map
import java.util.regex.Pattern
//import KineticaBulkLoader._
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import scala.collection.JavaConversions._
import com.kinetica.spark.LoaderParams
import org.apache.spark.Logging

class KineticaBulkLoader(lp: LoaderParams) extends Logging {

    def GetBulkInserter(): BulkInserter[GenericRecord] = {
        var bulkInserter: BulkInserter[GenericRecord] = null

        val gpudb = lp.getGpudb

        var workers: BulkInserter.WorkerList = getWorkers(gpudb)
        bulkInserter = new BulkInserter[GenericRecord](
            gpudb,
            lp.tablename,
            lp.tableType,
            lp.insertSize,
            getUpsertOptions,
            workers)

        bulkInserter.setRetryCount(lp.retryCount)
        bulkInserter
    }

    private def getUpsertOptions(): Map[String, String] = {
        val options: Map[String, String] = new HashMap[String, String]()
        options.put(
            "update_on_existing_pk",
            java.lang.Boolean.toString(lp.updateOnExistingPk))
        options
    }

    private def getWorkers(gpudb: GPUdb): BulkInserter.WorkerList = {

        // yes, this is a return in Scala and it is cleaner.
        // Scala purists can sue me :-)

        if(lp.isTableReplicated) {
            logInfo("Table is set to Is Replication: True")
            return null
        }

        if (!lp.multiHead) {
            logInfo("Multi-head ingest is turned off")
            return null
        }

        logInfo("multi-head ingest turned on")

        val pattern: Pattern = null
        if ((lp.KdbIpRegex != null) && !(lp.KdbIpRegex.trim().equalsIgnoreCase(""))) {
            logInfo("gpudbIpRegex not null: " + lp.KdbIpRegex)
            val pattern: Pattern = Pattern.compile(lp.KdbIpRegex)
        }

        var workers: BulkInserter.WorkerList = new BulkInserter.WorkerList(gpudb, pattern)
        if(workers.size == 0) {
            throw new Exception("No workers found")
        }

        var iter: Iterator[URL] = workers.iterator()
        for (url: URL <- workers) {
            logInfo("Worker:" + url)
        }

        workers
    }

}
