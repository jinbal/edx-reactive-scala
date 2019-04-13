package convert

import convert.RomanNumeralConverters.{ConversionResult, Five, Four, One}
import org.scalatest.Matchers

class RomanNumeralConvertersTest extends org.scalatest.FunSuite with Matchers {

  test("One should generate and empty  when remainning is zer") {
    val initial = ConversionResult(0)

    val conversionResult = One.append(initial)

    conversionResult.output shouldBe ""
  }

  test("One should generate the correct output") {
    val initialResult = ConversionResult(1)

    val convertionResult: ConversionResult = One.append(initialResult)

    convertionResult.remaining shouldBe 0
    convertionResult.output shouldBe "I"
  }

  test("Four should generate the correct output") {
    val initialResult = ConversionResult(5)

    val convertionResult: ConversionResult = Four.append(initialResult)

    convertionResult.remaining shouldBe 1
    convertionResult.output shouldBe "IV"
  }

  test("Five should generate the correct output") {
    val initialResult = ConversionResult(6)

    val convertionResult: ConversionResult = Five.append(initialResult)

    convertionResult.remaining shouldBe 1
    convertionResult.output shouldBe "V"
  }

  test("Roman numeral converter should convert correctly") {

    RomanNumeralConverters.convert(8).output shouldBe "VIII"
    RomanNumeralConverters.convert(10).output shouldBe "X"
    RomanNumeralConverters.convert(34).output shouldBe "XXXIV"
    RomanNumeralConverters.convert(169).output shouldBe "CLXIX"
    RomanNumeralConverters.convert(290).output shouldBe "CCXC"
    RomanNumeralConverters.convert(942).output shouldBe "CMXLII"
    RomanNumeralConverters.convert(1999).output shouldBe "MCMXCIX"
    RomanNumeralConverters.convert(2038).output shouldBe "MMXXXVIII"

  }


}
