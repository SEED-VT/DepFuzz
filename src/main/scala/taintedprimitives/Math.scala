package taintedprimitives

/**
  * Created by malig on 11/19/19.
  */
object MathSym {

  /** The `Double` value that is closer than any other to `e`, the base of
    *  the natural logarithms.
    *  @group math-const
    */
  @inline final val E = java.lang.Math.E

  /** The `Double` value that is closer than any other to `pi`, the ratio of
    *  the circumference of a circle to its diameter.
    *  @group math-const
    */
  @inline final val Pi = java.lang.Math.PI

  /** Returns a `Double` value with a positive sign, greater than or equal
    *  to `0.0` and less than `1.0`.
    *
    *  @group randomisation
    */
  def random(): Double = java.lang.Math.random()

  /**  @group trig */
  def sin(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.sin(x.value), x.getProvenance())

  /**  @group trig */
  def cos(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.cos(x.value), x.getProvenance())

  /**  @group trig */
  def tan(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.tan(x.value), x.getProvenance())

  /**  @group trig */
  def asin(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.asin(x.value), x.getProvenance())

  /**  @group trig */
  def acos(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.acos(x.value), x.getProvenance())

  /**  @group trig */
  def atan(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.atan(x.value), x.getProvenance())

  /** Converts an angle measured in degrees to an approximately equivalent
    *  angle measured in radians.
    *
    *  @param  x an angle, in degrees
    *  @return the measurement of the angle `x` in radians.
    *  @group angle-conversion
    */
  def toRadians(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.toRadians(x.value), x.getProvenance())

  /** Converts an angle measured in radians to an approximately equivalent
    *  angle measured in degrees.
    *
    *  @param  x angle, in radians
    *  @return the measurement of the angle `x` in degrees.
    *  @group angle-conversion
    */
  def toDegrees(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.toDegrees(x.value), x.getProvenance())

  /** Converts rectangular coordinates `(x, y)` to polar `(r, theta)`.
    *
    *  @param  x the ordinate coordinate
    *  @param  y the abscissa coordinate
    *  @return the ''theta'' component of the point `(r, theta)` in polar
    *          coordinates that corresponds to the point `(x, y)` in
    *          Cartesian coordinates.
    *  @group polar-coords
    */
  // TODO
  def atan2(y: Double, x: Double): Double = java.lang.Math.atan2(y, x)

  /** Returns the square root of the sum of the squares of both given `Double`
    * values without intermediate underflow or overflow.
    *
    * The ''r'' component of the point `(r, theta)` in polar
    * coordinates that corresponds to the point `(x, y)` in
    * Cartesian coordinates.
    * @group polar-coords
    */
  // TODO
  def hypot(x: Double, y: Double): Double = java.lang.Math.hypot(x, y)

  // -----------------------------------------------------------------------
  // rounding functions
  // -----------------------------------------------------------------------

  /** @group rounding */
  def ceil(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.ceil(x.value), x.getProvenance())

  /** @group rounding */
  def floor(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.floor(x.value), x.getProvenance())

  /** Returns the `Double` value that is closest in value to the
    *  argument and is equal to a mathematical integer.
    *
    *  @param  x a `Double` value
    *  @return the closest floating-point value to a that is equal to a
    *          mathematical integer.
    *  @group rounding
    */
  def rint(x: Double): Double = java.lang.Math.rint(x)

  /** There is no reason to round a `Long`, but this method prevents unintended conversion to `Float` followed by rounding to `Int`.
    *
    *  @note Does not forward to [[java.lang.Math]]
    *  @group rounding
    */
  @deprecated(
    "This is an integer type; there is no reason to round it. Perhaps you meant to call this with a floating-point value?",
    "2.11.0")
  def round(x: Long): Long = x

  /** Returns the closest `Int` to the argument.
    *
    *  @param  x a floating-point value to be rounded to a `Int`.
    *  @return the value of the argument rounded to the nearest `Int` value.
    *  @group rounding
    */
  def round(x: Float): Int = java.lang.Math.round(x)

  /** Returns the closest `Long` to the argument.
    *
    *  @param  x a floating-point value to be rounded to a `Long`.
    *  @return the value of the argument rounded to the nearest`long` value.
    *  @group rounding
    */
  def round(x: Double): Long = java.lang.Math.round(x)

  /** @group abs */
  def abs(x: TaintedInt): TaintedInt =
    new TaintedInt(Math.abs(x.value), x.getProvenance())

  /** @group abs */
  def abs(x: Long): Long = java.lang.Math.abs(x)

  /** @group abs */
  def abs(x: TaintedFloat): TaintedFloat =
     TaintedFloat(Math.abs(x.value), x.getProvenance())

  /** @group abs */
  def abs(x: TaintedDouble): TaintedDouble =
     TaintedDouble(Math.abs(x.value), x.getProvenance())

  /** @group minmax */
  def max(x: TaintedInt, y: TaintedInt): TaintedInt = {
    if (Math.max(x.value, y.value) == x.value)  x
    else  y
  }

  /** @group minmax */
  def max(x: Long, y: Long): Long = java.lang.Math.max(x, y)

  /** @group minmax */
  def max(x: TaintedFloat, y: TaintedFloat): TaintedFloat = {
    if (Math.max(x.value, y.value) == x.value)  x
    else return y
  }

  /** @group minmax */
  def max(x: TaintedDouble, y: TaintedDouble): TaintedDouble = {
    if (Math.max(x.value, y.value) == x.value)  x
    else  y
  }

  /** @group minmax */
  def min(x: TaintedInt, y: TaintedInt): TaintedInt = {
    if (Math.min(x.value, y.value) == x.value)  x
    else  y
  }

  /** @group minmax */
  def min(x: Long, y: Long): Long = java.lang.Math.min(x, y)

  /** @group minmax */
  def min(x: TaintedFloat, y: TaintedFloat): TaintedFloat = {
    if (Math.min(x.value, y.value) == x.value)  x
    else return y
  }

  /** @group minmax */
  def min(x: TaintedDouble, y: TaintedDouble): TaintedDouble = {
    if (Math.min(x.value, y.value) == x.value) return x
    else return y
  }

  /** @group signs
    * @note Forwards to [[java.lang.Integer]]
    */
  def signum(x: Int): Int = java.lang.Integer.signum(x)

  /** @group signs
    * @note Forwards to [[java.lang.Long]]
    */
  def signum(x: Long): Long = java.lang.Long.signum(x)

  /** @group signs */
  def signum(x: Float): Float = java.lang.Math.signum(x)

  /** @group signs */
  def signum(x: Double): Double = java.lang.Math.signum(x)

  /** @group modquo */
  def floorDiv(x: Int, y: Int): Int = java.lang.Math.floorDiv(x, y)

  /** @group modquo */
  def floorDiv(x: Long, y: Long): Long = java.lang.Math.floorDiv(x, y)

  /** @group modquo */
  def floorMod(x: Int, y: Int): Int = java.lang.Math.floorMod(x, y)

  /** @group modquo */
  def floorMod(x: Long, y: Long): Long = java.lang.Math.floorMod(x, y)

  /** @group signs */
  def copySign(magnitude: Double, sign: Double): Double =
    java.lang.Math.copySign(magnitude, sign)

  /** @group signs */
  def copySign(magnitude: Float, sign: Float): Float =
    java.lang.Math.copySign(magnitude, sign)

  /** @group adjacent-float */
  def nextAfter(start: Double, direction: Double): Double =
    java.lang.Math.nextAfter(start, direction)

  /** @group adjacent-float */
  def nextAfter(start: Float, direction: Double): Float =
    java.lang.Math.nextAfter(start, direction)

  /** @group adjacent-float */
  def nextUp(d: Double): Double = java.lang.Math.nextUp(d)

  /** @group adjacent-float */
  def nextUp(f: Float): Float = java.lang.Math.nextUp(f)

  /** @group adjacent-float */
  def nextDown(d: Double): Double = java.lang.Math.nextDown(d)

  /** @group adjacent-float */
  def nextDown(f: Float): Float = java.lang.Math.nextDown(f)

  /** @group scaling */
  def scalb(d: Double, scaleFactor: Int): Double =
    java.lang.Math.scalb(d, scaleFactor)

  /** @group scaling */
  def scalb(f: Float, scaleFactor: Int): Float =
    java.lang.Math.scalb(f, scaleFactor)

  // -----------------------------------------------------------------------
  // root functions
  // -----------------------------------------------------------------------

  /** Returns the square root of a `Double` value.
    *
    * @param  x the number to take the square root of
    * @return the value √x
    * @group root-extraction
    */
  def sqrt(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.sqrt(x.value), x.getProvenance())

  /** Returns the cube root of the given `Double` value.
    *
    * @param  x the number to take the cube root of
    * @return the value ∛x
    * @group root-extraction
    */
  def cbrt(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.cbrt(x.value), x.getProvenance())

  // -----------------------------------------------------------------------
  // exponential functions
  // -----------------------------------------------------------------------

  /** Returns the value of the first argument raised to the power of the
    *  second argument.
    *
    *  @param x the base.
    *  @param y the exponent.
    *  @return the value `x^y^`.
    *  @group explog
    */
  def pow(x: TaintedDouble, y: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.pow(x.value, y.value), x.newProvenance(y.getProvenance()))

  /** Returns Euler's number `e` raised to the power of a `Double` value.
    *
    *  @param  x the exponent to raise `e` to.
    *  @return the value `e^a^`, where `e` is the base of the natural
    *          logarithms.
    *  @group explog
    */
  def exp(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.exp(x.value), x.getProvenance())

  /** Returns `exp(x) - 1`.
    *  @group explog
    */
  def expm1(x: Double): Double = java.lang.Math.expm1(x)

  /** @group explog */
  def getExponent(f: Float): Int = java.lang.Math.getExponent(f)

  /** @group explog */
  def getExponent(d: Double): Int = java.lang.Math.getExponent(d)

  // -----------------------------------------------------------------------
  // logarithmic functions
  // -----------------------------------------------------------------------

  /** Returns the natural logarithm of a `Double` value.
    *
    *  @param  x the number to take the natural logarithm of
    *  @return the value `logₑ(x)` where `e` is Eulers number
    *  @group explog
    */
  def log(x: TaintedDouble): TaintedDouble =
    TaintedDouble(Math.log(x.value), x.getProvenance())

  /** Returns the natural logarithm of the sum of the given `Double` value and 1.
    *  @group explog
    */
  def log1p(x: Double): Double = java.lang.Math.log1p(x)

  /** Returns the base 10 logarithm of the given `Double` value.
    *  @group explog
    */
  def log10(x: Double): Double = java.lang.Math.log10(x)

  // -----------------------------------------------------------------------
  // trigonometric functions
  // -----------------------------------------------------------------------

  /** Returns the hyperbolic sine of the given `Double` value.
    * @group hyperbolic
    */
  def sinh(x: Double): Double = java.lang.Math.sinh(x)

  /** Returns the hyperbolic cosine of the given `Double` value.
    * @group hyperbolic
    */
  def cosh(x: Double): Double = java.lang.Math.cosh(x)

  /** Returns the hyperbolic tangent of the given `Double` value.
    * @group hyperbolic
    */
  def tanh(x: Double): Double = java.lang.Math.tanh(x)

  // -----------------------------------------------------------------------
  // miscellaneous functions
  // -----------------------------------------------------------------------

  /** Returns the size of an ulp of the given `Double` value.
    * @group ulp
    */
  def ulp(x: Double): Double = java.lang.Math.ulp(x)

  /** Returns the size of an ulp of the given `Float` value.
    * @group ulp
    */
  def ulp(x: Float): Float = java.lang.Math.ulp(x)

  /** @group exact */
  def IEEEremainder(x: Double, y: Double): Double =
    java.lang.Math.IEEEremainder(x, y)

  // -----------------------------------------------------------------------
  // exact functions
  // -----------------------------------------------------------------------

  /** @group exact */
  def addExact(x: Int, y: Int): Int = java.lang.Math.addExact(x, y)

  /** @group exact */
  def addExact(x: Long, y: Long): Long = java.lang.Math.addExact(x, y)

  /** @group exact */
  def subtractExact(x: Int, y: Int): Int = java.lang.Math.subtractExact(x, y)

  /** @group exact */
  def subtractExact(x: Long, y: Long): Long = java.lang.Math.subtractExact(x, y)

  /** @group exact */
  def multiplyExact(x: Int, y: Int): Int = java.lang.Math.multiplyExact(x, y)

  /** @group exact */
  def multiplyExact(x: Long, y: Long): Long = java.lang.Math.multiplyExact(x, y)

  /** @group exact */
  def incrementExact(x: Int): Int = java.lang.Math.incrementExact(x)

  /** @group exact */
  def incrementExact(x: Long) = java.lang.Math.incrementExact(x)

  /** @group exact */
  def decrementExact(x: Int) = java.lang.Math.decrementExact(x)

  /** @group exact */
  def decrementExact(x: Long) = java.lang.Math.decrementExact(x)

  /** @group exact */
  def negateExact(x: Int) = java.lang.Math.negateExact(x)

  /** @group exact */
  def negateExact(x: Long) = java.lang.Math.negateExact(x)

  /** @group exact */
  def toIntExact(x: Long): Int = java.lang.Math.toIntExact(x)

}
