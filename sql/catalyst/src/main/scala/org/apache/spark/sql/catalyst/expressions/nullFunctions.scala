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

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
<<<<<<< HEAD
=======
import org.apache.spark.sql.types.DataType
>>>>>>> githubspark/branch-1.3

case class Coalesce(children: Seq[Expression]) extends Expression {
  type EvaluatedType = Any

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
<<<<<<< HEAD
  def nullable = !children.exists(!_.nullable)

  // Coalesce is foldable if all children are foldable.
  override def foldable = !children.exists(!_.foldable)
=======
  override def nullable: Boolean = !children.exists(!_.nullable)

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = !children.exists(!_.foldable)
>>>>>>> githubspark/branch-1.3

  // Only resolved if all the children are of the same type.
  override lazy val resolved = childrenResolved && (children.map(_.dataType).distinct.size == 1)

<<<<<<< HEAD
  override def toString = s"Coalesce(${children.mkString(",")})"

  def dataType = if (resolved) {
=======
  override def toString: String = s"Coalesce(${children.mkString(",")})"

  override def dataType: DataType = if (resolved) {
>>>>>>> githubspark/branch-1.3
    children.head.dataType
  } else {
    val childTypes = children.map(c => s"$c: ${c.dataType}").mkString(", ")
    throw new UnresolvedException(
      this, s"Coalesce cannot have children of different types. $childTypes")
  }

  override def eval(input: Row): Any = {
    var i = 0
    var result: Any = null
    val childIterator = children.iterator
    while (childIterator.hasNext && result == null) {
      result = childIterator.next().eval(input)
    }
    result
  }
}

case class IsNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
<<<<<<< HEAD
  override def foldable = child.foldable
  def nullable = false
=======
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = false
>>>>>>> githubspark/branch-1.3

  override def eval(input: Row): Any = {
    child.eval(input) == null
  }

<<<<<<< HEAD
  override def toString = s"IS NULL $child"
}

case class IsNotNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  override def foldable = child.foldable
  def nullable = false
  override def toString = s"IS NOT NULL $child"
=======
  override def toString: String = s"IS NULL $child"
}

case class IsNotNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = false
  override def toString: String = s"IS NOT NULL $child"
>>>>>>> githubspark/branch-1.3

  override def eval(input: Row): Any = {
    child.eval(input) != null
  }
}
<<<<<<< HEAD
=======

/**
 * A predicate that is evaluated to be true if there are at least `n` non-null values.
 */
case class AtLeastNNonNulls(n: Int, children: Seq[Expression]) extends Predicate {
  override def nullable: Boolean = false
  override def foldable: Boolean = false
  override def toString: String = s"AtLeastNNulls(n, ${children.mkString(",")})"

  private[this] val childrenArray = children.toArray

  override def eval(input: Row): Boolean = {
    var numNonNulls = 0
    var i = 0
    while (i < childrenArray.length && numNonNulls < n) {
      if (childrenArray(i).eval(input) != null) {
        numNonNulls += 1
      }
      i += 1
    }
    numNonNulls >= n
  }
}
>>>>>>> githubspark/branch-1.3
