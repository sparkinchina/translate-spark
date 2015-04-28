/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
<<<<<<< HEAD
import org.apache.spark.sql.catalyst.types._
=======
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.types._
>>>>>>> githubspark/branch-1.3

case class UnaryMinus(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

<<<<<<< HEAD
  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"-$child"

  override def eval(input: Row): Any = {
    n1(child, input, _.negate(_))
=======
  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"-$child"

  lazy val numeric = dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.negate(evalE)
    }
>>>>>>> githubspark/branch-1.3
  }
}

case class Sqrt(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

<<<<<<< HEAD
  def dataType = DoubleType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"SQRT($child)"

  override def eval(input: Row): Any = {
    n1(child, input, (na,a) => math.sqrt(na.toDouble(a)))
=======
  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"SQRT($child)"

  lazy val numeric = child.dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support non-negative numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else math.sqrt(value)
    }
>>>>>>> githubspark/branch-1.3
  }
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  type EvaluatedType = Any

<<<<<<< HEAD
  def nullable = left.nullable || right.nullable
=======
  def nullable: Boolean = left.nullable || right.nullable
>>>>>>> githubspark/branch-1.3

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    !DecimalType.isFixed(left.dataType)

<<<<<<< HEAD
  def dataType = {
=======
  def dataType: DataType = {
>>>>>>> githubspark/branch-1.3
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        evalInternal(evalE1, evalE2)
      }
    }
  }

  def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any =
    sys.error(s"BinaryExpressions must either override eval or evalInternal")
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
<<<<<<< HEAD
  def symbol = "+"

  override def eval(input: Row): Any = n2(input, left, right, _.plus(_, _))
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "-"

  override def eval(input: Row): Any = n2(input, left, right, _.minus(_, _))
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "*"

  override def eval(input: Row): Any = n2(input, left, right, _.times(_, _))
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "/"

  override def nullable = true

  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    dataType match {
      case _ if evalE2 == null => null
      case _ if evalE2 == 0 => null
      case ft: FractionalType => f1(input, left, _.div(_, evalE2.asInstanceOf[ft.JvmType]))
      case it: IntegralType => i1(input, left, _.quot(_, evalE2.asInstanceOf[it.JvmType]))
    }
  }

}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  def symbol = "%"

  override def nullable = left.nullable || right.nullable || dataType.isInstanceOf[DecimalType]

  override def eval(input: Row): Any = i2(input, left, right, _.rem(_, _))
=======
  override def symbol: String = "+"

  lazy val numeric = dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.plus(evalE1, evalE2)
      }
    }
  }
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "-"

  lazy val numeric = dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.minus(evalE1, evalE2)
      }
    }
  }
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "*"

  lazy val numeric = dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.times(evalE1, evalE2)
      }
    }
  }
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "/"

  override def nullable: Boolean = true

  lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
    case it: IntegralType => it.integral.asInstanceOf[Integral[Any]].quot
    case other => sys.error(s"Type $other does not support numeric operations")
  }
  
  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        div(evalE1, evalE2)
      }
    }
  }
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "%"

  override def nullable: Boolean = true

  lazy val integral = dataType match {
    case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
    case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        integral.rem(evalE1, evalE2)
      }
    }
  }
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that calculates bitwise and(&) of two numbers.
 */
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic {
<<<<<<< HEAD
  def symbol = "&"

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = dataType match {
    case ByteType => (evalE1.asInstanceOf[Byte] & evalE2.asInstanceOf[Byte]).toByte
    case ShortType => (evalE1.asInstanceOf[Short] & evalE2.asInstanceOf[Short]).toShort
    case IntegerType => evalE1.asInstanceOf[Int] & evalE2.asInstanceOf[Int]
    case LongType => evalE1.asInstanceOf[Long] & evalE2.asInstanceOf[Long]
    case other => sys.error(s"Unsupported bitwise & operation on $other")
  }
=======
  override def symbol: String = "&"

  lazy val and: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 & evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 & evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
    case other => sys.error(s"Unsupported bitwise & operation on $other")
  }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = and(evalE1, evalE2)
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 */
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic {
<<<<<<< HEAD
  def symbol = "|"

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = dataType match {
    case ByteType => (evalE1.asInstanceOf[Byte] | evalE2.asInstanceOf[Byte]).toByte
    case ShortType => (evalE1.asInstanceOf[Short] | evalE2.asInstanceOf[Short]).toShort
    case IntegerType => evalE1.asInstanceOf[Int] | evalE2.asInstanceOf[Int]
    case LongType => evalE1.asInstanceOf[Long] | evalE2.asInstanceOf[Long]
    case other => sys.error(s"Unsupported bitwise | operation on $other")
  }
=======
  override def symbol: String = "|"

  lazy val or: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 | evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 | evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
    case other => sys.error(s"Unsupported bitwise | operation on $other")
  }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = or(evalE1, evalE2)
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that calculates bitwise xor(^) of two numbers.
 */
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic {
<<<<<<< HEAD
  def symbol = "^"

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = dataType match {
    case ByteType => (evalE1.asInstanceOf[Byte] ^ evalE2.asInstanceOf[Byte]).toByte
    case ShortType => (evalE1.asInstanceOf[Short] ^ evalE2.asInstanceOf[Short]).toShort
    case IntegerType => evalE1.asInstanceOf[Int] ^ evalE2.asInstanceOf[Int]
    case LongType => evalE1.asInstanceOf[Long] ^ evalE2.asInstanceOf[Long]
    case other => sys.error(s"Unsupported bitwise ^ operation on $other")
  }
=======
  override def symbol: String = "^"

  lazy val xor: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 ^ evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 ^ evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
    case other => sys.error(s"Unsupported bitwise ^ operation on $other")
  }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = xor(evalE1, evalE2)
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that calculates bitwise not(~) of a number.
 */
case class BitwiseNot(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

<<<<<<< HEAD
  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"~$child"
=======
  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"~$child"

  lazy val not: (Any) => Any = dataType match {
    case ByteType =>
      ((evalE: Byte) => (~evalE).toByte).asInstanceOf[(Any) => Any]
    case ShortType =>
      ((evalE: Short) => (~evalE).toShort).asInstanceOf[(Any) => Any]
    case IntegerType =>
      ((evalE: Int) => ~evalE).asInstanceOf[(Any) => Any]
    case LongType =>
      ((evalE: Long) => ~evalE).asInstanceOf[(Any) => Any]
    case other => sys.error(s"Unsupported bitwise ~ operation on $other")
  }
>>>>>>> githubspark/branch-1.3

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
<<<<<<< HEAD
      dataType match {
        case ByteType => (~evalE.asInstanceOf[Byte]).toByte
        case ShortType => (~evalE.asInstanceOf[Short]).toShort
        case IntegerType => ~evalE.asInstanceOf[Int]
        case LongType => ~evalE.asInstanceOf[Long]
        case other => sys.error(s"Unsupported bitwise ~ operation on $other")
      }
=======
      not(evalE)
>>>>>>> githubspark/branch-1.3
    }
  }
}

case class MaxOf(left: Expression, right: Expression) extends Expression {
  type EvaluatedType = Any

<<<<<<< HEAD
  override def foldable = left.foldable && right.foldable

  override def nullable = left.nullable && right.nullable

  override def children = left :: right :: Nil

  override def dataType = left.dataType

  override def eval(input: Row): Any = {
    val leftEval = left.eval(input)
    val rightEval = right.eval(input)
    if (leftEval == null) {
      rightEval
    } else if (rightEval == null) {
      leftEval
    } else {
      val numeric = left.dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
      if (numeric.compare(leftEval, rightEval) < 0) {
        rightEval
      } else {
        leftEval
=======
  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable && right.nullable

  override def children: Seq[Expression] = left :: right :: Nil

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  lazy val ordering = left.dataType match {
    case i: NativeType => i.ordering.asInstanceOf[Ordering[Any]]
    case other => sys.error(s"Type $other does not support ordered operations")
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    val evalE2 = right.eval(input)
    if (evalE1 == null) {
      evalE2
    } else if (evalE2 == null) {
      evalE1
    } else {
      if (ordering.compare(evalE1, evalE2) < 0) {
        evalE2
      } else {
        evalE1
>>>>>>> githubspark/branch-1.3
      }
    }
  }

<<<<<<< HEAD
  override def toString = s"MaxOf($left, $right)"
=======
  override def toString: String = s"MaxOf($left, $right)"
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that get the absolute value of the numeric value.
 */
case class Abs(child: Expression) extends UnaryExpression  {
  type EvaluatedType = Any

<<<<<<< HEAD
  def dataType = child.dataType
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"Abs($child)"

  override def eval(input: Row): Any = n1(child, input, _.abs(_))
=======
  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"Abs($child)"

  lazy val numeric = dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.abs(evalE)
    }
  }
>>>>>>> githubspark/branch-1.3
}
