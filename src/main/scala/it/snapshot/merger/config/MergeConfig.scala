package it.snapshot.merger.config

import it.snapshot.merger.mongo.MongoHelper
import scala.jdk.CollectionConverters._

object MergeConfig {
  private val mergeConfig = MongoHelper.getMergeConfig
  val hBaseStagingDir: String = mergeConfig.getEmbedded(List("config", "merger", "hbaseStagingDir", "value").asJava,
                                                        classOf[String])
  val hBaseTmpDirectory: String = mergeConfig.getEmbedded(List("config", "merger", "hbaseTemporaryDir", "value").asJava,
                                                        classOf[String])
  val hBaseNamespace: String = mergeConfig.getEmbedded(List("config", "merger", "hBaseNamespace", "value").asJava,
                                                        classOf[String])
}
