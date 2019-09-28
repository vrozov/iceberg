/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.util

import java.util.function.{BiPredicate, Function => JFunction, Predicate => JPredicate, Supplier => JSupplier}
import org.apache.iceberg.util.Tasks.{FailureTask, Task}

object JavaFunctionImplicits {

  implicit def toJavaFunction[A, B](function: A => B): JFunction[A, B] =
    new JFunction[A, B] {
      override def apply(a: A): B = function(a)
    }

  implicit def toJavaPredicate[A](predicate: A => Boolean): JPredicate[A] =
    new JPredicate[A] {
      override def test(a: A): Boolean = predicate(a)
    }

  implicit def toJavaBiPredicate[A, B](predicate: (A, B) => Boolean): BiPredicate[A, B] =
    new BiPredicate[A, B] {
      def test(a: A, b: B) = predicate(a, b)
    }

  implicit def toJavaSupplier[A](supplier: => A): JSupplier[A] =
    new JSupplier[A] {
      override def get(): A = supplier
    }

  implicit def toJavaTask[I, E <: Exception](task: I => Unit): Task[I, E] =
    new Task[I, E] {
      override def run(item: I): Unit = {
        task(item)
      }
    }

  implicit def toJavaFailureTask[I, E <: Exception](task: (I, Exception) => Unit): FailureTask[I, E] =
    new FailureTask[I, E] {
      override def run(item: I, exception: Exception): Unit = {
        task(item, exception)
      }
    }
}
