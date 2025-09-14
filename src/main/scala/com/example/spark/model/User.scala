package com.example.spark.model

import java.sql.Timestamp

case class User(
  id: Long,
  name: String,
  email: String,
  created_at: Timestamp,
  updated_at: Option[Timestamp] = None,
  is_active: Boolean = true
)

object User {
  def apply(id: Long, name: String, email: String): User = {
    val now = new Timestamp(System.currentTimeMillis())
    User(id, name, email, now, None, true)
  }
}