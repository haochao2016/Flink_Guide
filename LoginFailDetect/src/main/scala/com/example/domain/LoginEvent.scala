package com.example.domain

// define the case class for login event
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)
// define the case class for login fail message
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

