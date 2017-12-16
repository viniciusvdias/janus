package br.ufmg.cs.systems.common

// log
import org.apache.log4j.{Level, Logger}

trait Logging {
  private val logger = Logger.getLogger (this.getClass.getSimpleName)

  protected def logInfo(msg: String): Unit = {
    logger.info (msg)
  }

  protected def logWarning(msg: String): Unit = {
    logger.warn (msg)
  }

  protected def logDebug(msg: String): Unit = {
    logger.debug (msg)
  }

  protected def logError(msg: String): Unit = {
    logger.error (msg)
  }
}
