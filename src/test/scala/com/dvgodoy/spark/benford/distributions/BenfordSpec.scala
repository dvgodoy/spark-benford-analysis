package com.dvgodoy.spark.benford.distributions

import org.scalatest._
import play.api.Play
import play.api.test._
import play.api.test.Helpers._
import org.scalatestplus.play._

class BenfordSpec extends PlaySpec with OneAppPerSuite {
  // Override app if you need a FakeApplication with other than
  // default parameters.
  /*implicit override lazy val app: FakeApplication =
    FakeApplication(
      additionalConfiguration = Map("ehcacheplugin" -> "disabled")
    )

  "The OneAppPerSuite trait" must {
    "provide a FakeApplication" in {
      app.configuration.getString("ehcacheplugin") mustBe Some("disabled")
    }
    "start the FakeApplication" in {
      Play.maybeApplication mustBe Some(app)
    }
  }*/
}
