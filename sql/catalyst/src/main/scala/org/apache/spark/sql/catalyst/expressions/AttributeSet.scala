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

<<<<<<< HEAD
import org.apache.spark.sql.catalyst.analysis.Star

protected class AttributeEquals(val a: Attribute) {
  override def hashCode() = a.exprId.hashCode()
  override def equals(other: Any) = (a, other.asInstanceOf[AttributeEquals].a) match {
=======

protected class AttributeEquals(val a: Attribute) {
  override def hashCode(): Int = a match {
    case ar: AttributeReference => ar.exprId.hashCode()
    case a => a.hashCode()
  }

  override def equals(other: Any): Boolean = (a, other.asInstanceOf[AttributeEquals].a) match {
>>>>>>> githubspark/branch-1.3
    case (a1: AttributeReference, a2: AttributeReference) => a1.exprId == a2.exprId
    case (a1, a2) => a1 == a2
  }
}

object AttributeSet {
<<<<<<< HEAD
  def apply(a: Attribute) =
    new AttributeSet(Set(new AttributeEquals(a)))

  /** Constructs a new [[AttributeSet]] given a sequence of [[Expression Expressions]]. */
  def apply(baseSet: Seq[Expression]) =
=======
  def apply(a: Attribute): AttributeSet = new AttributeSet(Set(new AttributeEquals(a)))

  /** Constructs a new [[AttributeSet]] given a sequence of [[Expression Expressions]]. */
  def apply(baseSet: Iterable[Expression]): AttributeSet = {
>>>>>>> githubspark/branch-1.3
    new AttributeSet(
      baseSet
        .flatMap(_.references)
        .map(new AttributeEquals(_)).toSet)
<<<<<<< HEAD
=======
  }
>>>>>>> githubspark/branch-1.3
}

/**
 * A Set designed to hold [[AttributeReference]] objects, that performs equality checking using
 * expression id instead of standard java equality.  Using expression id means that these
 * sets will correctly test for membership, even when the AttributeReferences in question differ
 * cosmetically (e.g., the names have different capitalizations).
 *
 * Note that we do not override equality for Attribute references as it is really weird when
 * `AttributeReference("a"...) == AttrributeReference("b", ...)`. This tactic leads to broken tests,
 * and also makes doing transformations hard (we always try keep older trees instead of new ones
 * when the transformation was a no-op).
 */
class AttributeSet private (val baseSet: Set[AttributeEquals])
  extends Traversable[Attribute] with Serializable {

  /** Returns true if the members of this AttributeSet and other are the same. */
<<<<<<< HEAD
  override def equals(other: Any) = other match {
    case otherSet: AttributeSet => baseSet.map(_.a).forall(otherSet.contains)
=======
  override def equals(other: Any): Boolean = other match {
    case otherSet: AttributeSet =>
      otherSet.size == baseSet.size && baseSet.map(_.a).forall(otherSet.contains)
>>>>>>> githubspark/branch-1.3
    case _ => false
  }

  /** Returns true if this set contains an Attribute with the same expression id as `elem` */
  def contains(elem: NamedExpression): Boolean =
    baseSet.contains(new AttributeEquals(elem.toAttribute))

  /** Returns a new [[AttributeSet]] that contains `elem` in addition to the current elements. */
  def +(elem: Attribute): AttributeSet =  // scalastyle:ignore
    new AttributeSet(baseSet + new AttributeEquals(elem))

  /** Returns a new [[AttributeSet]] that does not contain `elem`. */
  def -(elem: Attribute): AttributeSet =
    new AttributeSet(baseSet - new AttributeEquals(elem))

  /** Returns an iterator containing all of the attributes in the set. */
  def iterator: Iterator[Attribute] = baseSet.map(_.a).iterator

  /**
   * Returns true if the [[Attribute Attributes]] in this set are a subset of the Attributes in
   * `other`.
   */
<<<<<<< HEAD
  def subsetOf(other: AttributeSet) = baseSet.subsetOf(other.baseSet)
=======
  def subsetOf(other: AttributeSet): Boolean = baseSet.subsetOf(other.baseSet)
>>>>>>> githubspark/branch-1.3

  /**
   * Returns a new [[AttributeSet]] that does not contain any of the [[Attribute Attributes]] found
   * in `other`.
   */
<<<<<<< HEAD
  def --(other: Traversable[NamedExpression]) =
=======
  def --(other: Traversable[NamedExpression]): AttributeSet =
>>>>>>> githubspark/branch-1.3
    new AttributeSet(baseSet -- other.map(a => new AttributeEquals(a.toAttribute)))

  /**
   * Returns a new [[AttributeSet]] that contains all of the [[Attribute Attributes]] found
   * in `other`.
   */
<<<<<<< HEAD
  def ++(other: AttributeSet) = new AttributeSet(baseSet ++ other.baseSet)
=======
  def ++(other: AttributeSet): AttributeSet = new AttributeSet(baseSet ++ other.baseSet)
>>>>>>> githubspark/branch-1.3

  /**
   * Returns a new [[AttributeSet]] contain only the [[Attribute Attributes]] where `f` evaluates to
   * true.
   */
<<<<<<< HEAD
  override def filter(f: Attribute => Boolean) = new AttributeSet(baseSet.filter(ae => f(ae.a)))
=======
  override def filter(f: Attribute => Boolean): AttributeSet =
    new AttributeSet(baseSet.filter(ae => f(ae.a)))
>>>>>>> githubspark/branch-1.3

  /**
   * Returns a new [[AttributeSet]] that only contains [[Attribute Attributes]] that are found in
   * `this` and `other`.
   */
<<<<<<< HEAD
  def intersect(other: AttributeSet) = new AttributeSet(baseSet.intersect(other.baseSet))
=======
  def intersect(other: AttributeSet): AttributeSet =
    new AttributeSet(baseSet.intersect(other.baseSet))
>>>>>>> githubspark/branch-1.3

  override def foreach[U](f: (Attribute) => U): Unit = baseSet.map(_.a).foreach(f)

  // We must force toSeq to not be strict otherwise we end up with a [[Stream]] that captures all
  // sorts of things in its closure.
  override def toSeq: Seq[Attribute] = baseSet.map(_.a).toArray.toSeq

<<<<<<< HEAD
  override def toString = "{" + baseSet.map(_.a).mkString(", ") + "}"
=======
  override def toString: String = "{" + baseSet.map(_.a).mkString(", ") + "}"

  override def isEmpty: Boolean = baseSet.isEmpty
>>>>>>> githubspark/branch-1.3
}
