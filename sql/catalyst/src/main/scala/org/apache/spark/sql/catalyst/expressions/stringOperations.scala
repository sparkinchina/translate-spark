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

import java.util.regex.Pattern

import scala.collection.IndexedSeqOptimized


import org.apache.spark.sql.catalyst.analysis.UnresolvedException
<<<<<<< HEAD
import org.apache.spark.sql.catalyst.types.{BinaryType, BooleanType, DataType, StringType}
=======
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, StringType}
>>>>>>> githubspark/branch-1.3

trait StringRegexExpression {
  self: BinaryExpression =>

  type EvaluatedType = Any

  def escape(v: String): String
  def matches(regex: Pattern, str: String): Boolean

<<<<<<< HEAD
  def nullable: Boolean = left.nullable || right.nullable
  def dataType: DataType = BooleanType
=======
  override def nullable: Boolean = left.nullable || right.nullable
  override def dataType: DataType = BooleanType
>>>>>>> githubspark/branch-1.3

  // try cache the pattern for Literal
  private lazy val cache: Pattern = right match {
    case x @ Literal(value: String, StringType) => compile(value)
    case _ => null
  }

  protected def compile(str: String): Pattern = if(str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str))
  }

  protected def pattern(str: String) = if(cache == null) compile(str) else cache

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    if (l == null) {
      null
    } else {
      val r = right.eval(input)
      if(r == null) {
        null
      } else {
        val regex = pattern(r.asInstanceOf[String])
        if(regex == null) {
          null
        } else {
          matches(regex, l.asInstanceOf[String])
        }
      }
    }
  }
}

trait CaseConversionExpression {
  self: UnaryExpression =>

  type EvaluatedType = Any

  def convert(v: String): String

  override def foldable: Boolean = child.foldable
  def nullable: Boolean = child.nullable
  def dataType: DataType = StringType

  override def eval(input: Row): Any = {
    val evaluated = child.eval(input)
    if (evaluated == null) {
      null
    } else {
      convert(evaluated.toString)
    }
  }
}

/**
 * Simple RegEx pattern matching function
 */
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

<<<<<<< HEAD
  def symbol = "LIKE"

  // replace the _ with .{1} exactly match 1 time of any character
  // replace the % with .*, match 0 or more times with any character
  override def escape(v: String) =
=======
  override def symbol: String = "LIKE"

  // replace the _ with .{1} exactly match 1 time of any character
  // replace the % with .*, match 0 or more times with any character
  override def escape(v: String): String =
>>>>>>> githubspark/branch-1.3
    if (!v.isEmpty) {
      "(?s)" + (' ' +: v.init).zip(v).flatMap {
        case (prev, '\\') => ""
        case ('\\', c) =>
          c match {
            case '_' => "_"
            case '%' => "%"
            case _ => Pattern.quote("\\" + c)
          }
        case (prev, c) =>
          c match {
            case '_' => "."
            case '%' => ".*"
            case _ => Pattern.quote(Character.toString(c))
          }
      }.mkString
    } else {
      v
    }

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()
}

case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

<<<<<<< HEAD
  def symbol = "RLIKE"
=======
  override def symbol: String = "RLIKE"
>>>>>>> githubspark/branch-1.3
  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
}

/**
 * A function that converts the characters of a string to uppercase.
 */
case class Upper(child: Expression) extends UnaryExpression with CaseConversionExpression {
  
  override def convert(v: String): String = v.toUpperCase()

<<<<<<< HEAD
  override def toString() = s"Upper($child)"
=======
  override def toString: String = s"Upper($child)"
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that converts the characters of a string to lowercase.
 */
case class Lower(child: Expression) extends UnaryExpression with CaseConversionExpression {
  
  override def convert(v: String): String = v.toLowerCase()

<<<<<<< HEAD
  override def toString() = s"Lower($child)"
=======
  override def toString: String = s"Lower($child)"
>>>>>>> githubspark/branch-1.3
}

/** A base trait for functions that compare two strings, returning a boolean. */
trait StringComparison {
  self: BinaryExpression =>

  type EvaluatedType = Any

<<<<<<< HEAD
  def nullable: Boolean = left.nullable || right.nullable
=======
  override def nullable: Boolean = left.nullable || right.nullable
>>>>>>> githubspark/branch-1.3
  override def dataType: DataType = BooleanType

  def compare(l: String, r: String): Boolean

  override def eval(input: Row): Any = {
    val leftEval = left.eval(input).asInstanceOf[String]
    if(leftEval == null) {
      null
    } else {
      val rightEval = right.eval(input).asInstanceOf[String]
      if (rightEval == null) null else compare(leftEval, rightEval)
    }
  }

<<<<<<< HEAD
  def symbol: String = nodeName

  override def toString() = s"$nodeName($left, $right)"
=======
  override def symbol: String = nodeName

  override def toString: String = s"$nodeName($left, $right)"
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that returns true if the string `left` contains the string `right`.
 */
case class Contains(left: Expression, right: Expression)
    extends BinaryExpression with StringComparison {
<<<<<<< HEAD
  override def compare(l: String, r: String) = l.contains(r)
=======
  override def compare(l: String, r: String): Boolean = l.contains(r)
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that returns true if the string `left` starts with the string `right`.
 */
case class StartsWith(left: Expression, right: Expression)
    extends BinaryExpression with StringComparison {
<<<<<<< HEAD
  def compare(l: String, r: String) = l.startsWith(r)
=======
  override def compare(l: String, r: String): Boolean = l.startsWith(r)
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that returns true if the string `left` ends with the string `right`.
 */
case class EndsWith(left: Expression, right: Expression)
    extends BinaryExpression with StringComparison {
<<<<<<< HEAD
  def compare(l: String, r: String) = l.endsWith(r)
=======
  override def compare(l: String, r: String): Boolean = l.endsWith(r)
>>>>>>> githubspark/branch-1.3
}

/**
 * A function that takes a substring of its first argument starting at a given position.
 * Defined for String and Binary types.
 */
case class Substring(str: Expression, pos: Expression, len: Expression) extends Expression {
  
  type EvaluatedType = Any

<<<<<<< HEAD
  override def foldable = str.foldable && pos.foldable && len.foldable

  def nullable: Boolean = str.nullable || pos.nullable || len.nullable
  def dataType: DataType = {
=======
  override def foldable: Boolean = str.foldable && pos.foldable && len.foldable

  override  def nullable: Boolean = str.nullable || pos.nullable || len.nullable
  override def dataType: DataType = {
>>>>>>> githubspark/branch-1.3
    if (!resolved) {
      throw new UnresolvedException(this, s"Cannot resolve since $children are not resolved")
    }
    if (str.dataType == BinaryType) str.dataType else StringType
  }

<<<<<<< HEAD
  override def children = str :: pos :: len :: Nil
=======
  override def children: Seq[Expression] = str :: pos :: len :: Nil
>>>>>>> githubspark/branch-1.3

  @inline
  def slice[T, C <: Any](str: C, startPos: Int, sliceLen: Int)
      (implicit ev: (C=>IndexedSeqOptimized[T,_])): Any = {
    val len = str.length
    // Hive and SQL use one-based indexing for SUBSTR arguments but also accept zero and
    // negative indices for start positions. If a start index i is greater than 0, it 
    // refers to element i-1 in the sequence. If a start index i is less than 0, it refers
    // to the -ith element before the end of the sequence. If a start index i is 0, it
    // refers to the first element.

    val start = startPos match {
      case pos if pos > 0 => pos - 1
      case neg if neg < 0 => len + neg
      case _ => 0
    }

    val end = sliceLen match {
      case max if max == Integer.MAX_VALUE => max
      case x => start + x
    }

    str.slice(start, end)    
  }

  override def eval(input: Row): Any = {
    val string = str.eval(input)

    val po = pos.eval(input)
    val ln = len.eval(input)

    if ((string == null) || (po == null) || (ln == null)) {
      null
    } else {
      val start = po.asInstanceOf[Int]
      val length = ln.asInstanceOf[Int] 

      string match {
        case ba: Array[Byte] => slice(ba, start, length)
        case other => slice(other.toString, start, length)
      }
    }
  }

<<<<<<< HEAD
  override def toString = len match {
=======
  override def toString: String = len match {
    // TODO: This is broken because max is not an integer value.
>>>>>>> githubspark/branch-1.3
    case max if max == Integer.MAX_VALUE => s"SUBSTR($str, $pos)"
    case _ => s"SUBSTR($str, $pos, $len)"
  }
}
