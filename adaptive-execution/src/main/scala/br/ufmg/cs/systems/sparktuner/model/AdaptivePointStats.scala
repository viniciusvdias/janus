package br.ufmg.cs.systems.sparktuner.model

import br.ufmg.cs.systems.sparktuner.Analyzer._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 */
case class AdaptivePointStats(
  rdd: RDD,
  stages: List[Stage]
  ) {

  override def toString = {
    s"AdaptivePointStats(rdd=${rdd},numStages=${stages.size})"
  }
}

object AdaptivePointStats {
}
