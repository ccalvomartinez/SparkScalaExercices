package com.carolina.calvo.model

import org.joda.time.DateTime

case class TransactionRecord(transactionDate: DateTime,
                             product: String,
                             price: Int,
                             paymentType: String,
                             name: String,
                             city: String,
                             ccc: Int,
                             lastLogin: DateTime,
                             latitude: Double,
                             longitude: Double,
                             description: String) {

}
