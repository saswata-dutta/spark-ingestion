package com.saswata.sparkseed.util.aws

import com.amazonaws.services.simplesystemsmanagement.model.{GetParametersByPathRequest, Parameter}
import com.amazonaws.services.simplesystemsmanagement.{
  AWSSimpleSystemsManagement,
  AWSSimpleSystemsManagementClient
}

import scala.collection.JavaConverters._

object SsmClient {

  def getParamsByPath(prefix: String, region: String): Map[String, String] = {
    require(prefix.startsWith("/"), "SSM path must start with /")
    require(prefix.endsWith("/"), "SSM path must end with /")

    val request =
      new GetParametersByPathRequest()
        .withWithDecryption(true)
        .withRecursive(true)
        .withPath(prefix)
    val response = ssmClient(region).getParametersByPath(request)
    response.getParameters.asScala.map(extractNameValue(prefix)).toMap
  }

  private def ssmClient(region: String): AWSSimpleSystemsManagement =
    AWSSimpleSystemsManagementClient.builder().withRegion(region).build()

  private def extractNameValue(prefix: String)(parameter: Parameter): (String, String) =
    (parameter.getName.stripPrefix(prefix), parameter.getValue)
}
