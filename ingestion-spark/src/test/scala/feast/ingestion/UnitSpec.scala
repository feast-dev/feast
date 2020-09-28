package feast.ingestion

import org.scalatest._
import matchers._
import org.scalatest.flatspec.AnyFlatSpec

abstract class UnitSpec extends AnyFlatSpec with should.Matchers with
  OptionValues with Inside with Inspectors
