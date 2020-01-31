import AllClasses.OMTransactionKPI
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object CalculKPI extends App {

  val spark = SparkSession.builder()
    .appName("Calcul KPI")
    .master("local")
    .getOrCreate()

  import  spark.implicits._

  val kpiDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .csv("src/main/resources/OM_TRANSACTIONSUSERS_20200120_1.csv")
    .as[OMTransactionKPI]


  // Calcul du montant de la transaction
  var MNT_TRANSACTION = "MNT_TRANSACTION"
  case class MontantTransac (MNT_TRANSACTION : Int)
  val mntTransacDS = kpiDS.select("MNT_TRANSACTION").as[MontantTransac]
  val mntTransac = mntTransacDS.toDF("MNT_TRANSACTION")
    .agg(sum("MNT_TRANSACTION")).first().get(0)
  //print("Le montant de la transaction est de: " + mntTransac)

  //Calcul du montant de la commission payé
  var COMMISSIONS_PAID = "COMMISSIONS_PAID"
  case class CommissionPaid (COMMISSIONS_PAID : Int)
  val mntComPaidDS = kpiDS.select("COMMISSIONS_PAID").as[CommissionPaid]
  val mntComPaid = mntComPaidDS.toDF("COMMISSIONS_PAID").agg(sum("COMMISSIONS_PAID")).first().get(0)
  //println("Le montant de la commission payee est de: " + mntComPaid)

  //Calcul du montant de la commission recue
  var COMMISSIONS_RECEIVED = "COMMISSIONS_RECEIVED"
  case class CommissionReceived (COMMISSIONS_RECEIVED : Int)
  val mntComReceivedDS = kpiDS.select("COMMISSIONS_RECEIVED").as[CommissionReceived]
  val mntComReceived = mntComReceivedDS.toDF("COMMISSIONS_RECEIVED")
    .agg(sum("COMMISSIONS_RECEIVED")).first().get(0)
  //println("Le montant de la commission recue est de: " + mntComReceived)

  //Calcul du montant des autres commissions
  var COMMISSIONS_OTHERS = "COMMISSIONS_OTHERS"
  case class CommissionsOthers (COMMISSIONS_OTHERS : Int)
  val mntComOthersDS = kpiDS.select("COMMISSIONS_OTHERS").as[CommissionsOthers]
  val mntComOthers = mntComOthersDS.toDF("COMMISSIONS_OTHERS")
    .agg(sum("COMMISSIONS_OTHERS")).first().get(0)
  //println("Le montant des autres commissions est de: " + mntComOthers)

  // Calcul du montant des services recus
  var SERVICE_CHARGE_RECEIVED = "SERVICE_CHARGE_RECEIVED"
  case class MontantServicesReceived (SERVICE_CHARGE_RECEIVED : Int)
  val mntServicesReceivedDS = kpiDS.select("SERVICE_CHARGE_RECEIVED").as[MontantServicesReceived]
  val mntServicesReceived = mntServicesReceivedDS.toDF("SERVICE_CHARGE_RECEIVED")
    .agg(sum("SERVICE_CHARGE_RECEIVED")).first().get(0)
  //println("Le montant des services recus est de: " + mntServicesReceived)

  // Calcul du montant des services payees
  var SERVICE_CHARGE_PAID = "SERVICE_CHARGE_PAID"
  case class MontantServicesPaid (SERVICE_CHARGE_PAID : Int)
  val mntServicesPaidDS = kpiDS.select("SERVICE_CHARGE_PAID").as[MontantServicesPaid]
  val mntServicesPaid = mntServicesPaidDS.toDF("SERVICE_CHARGE_PAID")
    .agg(sum("SERVICE_CHARGE_PAID")).first().get(0)
  //println("Le montant des services payes est de: " + mntServicesPaid)

  // Calcul des taxes
  var TAXES = "TAXES"
  case class Taxes (TAXES : Int)
  val mntTaxesDS = kpiDS.select("TAXES").as[Taxes]
  val mntTaxes = mntTaxesDS.toDF("TAXES").agg(sum("TAXES")).first().get(0)
  //println("Le montant des taxes est de: " + mntTaxes)

  // Calcul du montant des SENDER_PRE_BAL
  var SENDER_PRE_BAL = "SENDER_PRE_BAL"
  case class SenderPreBal (SENDER_PRE_BAL : Long)
  val mntSenderPreBalDS = kpiDS.select("SENDER_PRE_BAL").as[SenderPreBal]
  val mntSenderPreBal = mntSenderPreBalDS.toDF("SENDER_PRE_BAL")
    .agg(sum("SENDER_PRE_BAL")).first().get(0)
  //println("Le montant des SENDER_PRE_BAL est de: " + mntSenderPreBal)

  // Calcul du montant des SENDER_POST_BAL
  var SENDER_POST_BAL = "SENDER_POST_BAL"
  case class SenderPostBal (SENDER_POST_BAL : Long)
  val mntSenderPostBalDS = kpiDS.select("SENDER_POST_BAL").as[SenderPostBal]
  val mntSenderPostBal = mntSenderPostBalDS.toDF("SENDER_POST_BAL")
    .agg(sum("SENDER_POST_BAL")).first().get(0)
  //println("Le montant des SENDER_POST_BAL est de: " + mntSenderPostBal)

  // Calcul du montant des RECEIVER_PRE_BAL
  var RECEIVER_PRE_BAL = "RECEIVER_PRE_BAL"
  case class ReceiverPreBal (RECEIVER_PRE_BAL : Long)
  val mntReceiverPreBalDS = kpiDS.select("RECEIVER_PRE_BAL").as[ReceiverPreBal]
  val mntReceiverPreBal = mntReceiverPreBalDS.toDF("RECEIVER_PRE_BAL")
    .agg(sum("RECEIVER_PRE_BAL")).first().get(0)
  //println("Le montant des RECEIVER_PRE_BAL est de: " + mntReceiverPreBal)

  // Calcul du montant des RECEIVER_POST_BAL
  var RECEIVER_POST_BAL = "RECEIVER_POST_BAL"
  case class ReceiverPostBal (RECEIVER_POST_BAL : Long)
  val mntReceiverPostBalDS = kpiDS.select("RECEIVER_POST_BAL").as[ReceiverPostBal]
  val mntReceiverPostBal = mntReceiverPostBalDS.toDF("RECEIVER_POST_BAL")
    .agg(sum("RECEIVER_POST_BAL")).first().get(0)
  //println("Le montant des RECEIVER_POST_BAL est de: " + mntReceiverPostBal)


  // Ecriture des KPI dans un fichier CSV
  
  import java.io.{BufferedWriter, FileWriter}
  import scala.collection.JavaConversions._
  import scala.collection.mutable.ListBuffer
  import scala.util.Random
  import au.com.bytecode.opencsv.CSVWriter

  val outputFile = new BufferedWriter(new FileWriter("src/main/resources/OM_KPI.csv"))
  val csvWriter = new CSVWriter(outputFile)
  val csvHeader = Array("KPI", "MONTANT")
  var listOfKPI = new ListBuffer[Array[String]]()

  listOfKPI += csvHeader
  listOfKPI += Array(MNT_TRANSACTION, mntTransac.toString)
  listOfKPI += Array(COMMISSIONS_PAID, mntComPaid.toString)
  listOfKPI += Array(COMMISSIONS_RECEIVED, mntComReceived.toString)
  listOfKPI += Array(COMMISSIONS_OTHERS, mntComOthers.toString)
  listOfKPI += Array(SERVICE_CHARGE_RECEIVED, mntServicesReceived.toString)
  listOfKPI += Array(SERVICE_CHARGE_PAID, mntServicesPaid.toString)
  listOfKPI += Array(TAXES, mntTaxes.toString)
  listOfKPI += Array(SENDER_PRE_BAL, mntSenderPreBal.toString)
  listOfKPI += Array(SENDER_POST_BAL, mntSenderPostBal.toString)
  listOfKPI += Array(RECEIVER_PRE_BAL, mntReceiverPreBal.toString)
  listOfKPI += Array(RECEIVER_POST_BAL, mntReceiverPostBal.toString)

  csvWriter.writeAll(listOfKPI.toList)
  outputFile.close()
}
