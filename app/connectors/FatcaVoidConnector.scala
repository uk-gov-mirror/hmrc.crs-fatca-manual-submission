/*
 * Copyright 2025 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package connectors

import config.AppConfig
import models.VoidRequestPayload
import play.api.Logging
import play.api.http.Status.*
import play.api.libs.json.Json
import play.api.libs.ws.JsonBodyWritables.*
import uk.gov.hmrc.http.client.HttpClientV2
import uk.gov.hmrc.http.{HeaderCarrier, HttpResponse, StringContextOps, UpstreamErrorResponse}
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}
import uk.gov.hmrc.http.HttpReads.Implicits.*

class FatcaVoidConnector @Inject()(val config: AppConfig, val httpClient: HttpClientV2)(implicit ec: ExecutionContext) extends Logging {

    def submit(requestBody: VoidRequestPayload)(implicit hc: HeaderCarrier): Future[Unit] = {
    val serviceName = "void-fatca-submission"
    val endpoint    = url"${config.voidFatcaSubmission}/dac6/submitVoidRequest/v1"
    val headers =
      Seq()
        .withAccept()
        .withBearerToken(config.bearerToken(serviceName))
        .withContentType()
        .withDate()
        .withXCorrelationId()
        .withXForwardedHost()
        .withXConversationId()
    httpClient
      .post(endpoint)
      .setHeader(headers: _*)
      .withBody(Json.toJson(requestBody))
      .execute[HttpResponse]
      .flatMap { response =>
        response.status match {
          case NO_CONTENT => Future.successful(())
          case status =>
            val mayBeCorrelationId: Option[String] =
              headers.collectFirst {
                case (name, value) if name.equalsIgnoreCase("x-correlation-Id") => value
              }
            logger.error(
              s"""
                 |EIS API call failed
                 |Status: $status
                 |CorrelationId: ${mayBeCorrelationId.getOrElse("not-present")}
                 |Response body: ${response.body}
                 |""".stripMargin)
            Future.failed(UpstreamErrorResponse(
              message = s"Unexpected response from EIS API: $status",
              statusCode = INTERNAL_SERVER_ERROR,
              reportAs = INTERNAL_SERVER_ERROR
            ))
        }
      }

    }
}
