package convert

object RomanNumeralConverters {

  case class ConversionResult(remaining: Int, output: String = "")

  sealed abstract class NumeralSymbol(val symbol: String,
                                      val numericValue: Int) {

    def append(currentResult: ConversionResult) = {
      val remainder = currentResult.remaining % numericValue
      val modulus = (currentResult.remaining - remainder) / numericValue
      currentResult.copy(remainder, currentResult.output + List.fill(modulus)(symbol).mkString)
    }
  }

  case object One extends NumeralSymbol("I", 1)

  case object Four extends NumeralSymbol("IV", 4)

  case object Five extends NumeralSymbol("V", 5)

  case object Nine extends NumeralSymbol("IX", 9)

  case object Ten extends NumeralSymbol("X", 10)

  case object Forty extends NumeralSymbol("XL", 40)

  case object Fifty extends NumeralSymbol("L", 50)

  case object Ninety extends NumeralSymbol("XC", 90)

  case object Hundred extends NumeralSymbol("C", 100)

  case object FourHundred extends NumeralSymbol("CD", 400)

  case object NineHundred extends NumeralSymbol("CM", 900)

  case object Thousand extends NumeralSymbol("M", 1000)


  val RomanNumeralParsers = List(
                                 Thousand,
                                 NineHundred,
                                 FourHundred,
                                 Hundred,
                                 Ninety,
                                 Fifty,
                                 Forty,
                                 Ten,
                                 Nine,
                                 Five,
                                 Four,
                                 One
                               )

  def convert(numericValue: Int): ConversionResult = {
    RomanNumeralParsers
    .foldLeft(ConversionResult(numericValue)) { case (conversionResult, numeral) =>
      numeral.append(conversionResult)

    }
  }
}
