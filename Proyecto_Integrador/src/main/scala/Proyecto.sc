import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import kantan.csv.java8.localDateTimeDecoder

import scala.util.Try
import scala.util.Success
import scala.util.Failure

val path2DataFile = "D:/TIMO/Desktop/UTPL/ods_1_2.csv"
val formatDateTime = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")

case class Tweet(
                  idStr: String,
                  fromUser: String,
                  text: String,
                  createdAt: String,
                  time: LocalDateTime,
                  geoCoordinates: String,
                  userLang: String,
                  inReply2UserId: String,
                  inReply2ScreenName: String,
                  fromUserId: String,
                  inReply2StatusId: String,
                  source: String,
                  profileImageURL: String,
                  userFollowersCount: Double,
                  userFriendsCount: Double,
                  userLocation: String,
                  statusURL: String,
                  entitiesStr: String
                )
implicit val decoder : CellDecoder[LocalDateTime] = localDateTimeDecoder(formatDateTime)
val dataSource = new File(path2DataFile).readCsv[List, Tweet](rfc.withHeader)
val values = dataSource.collect({ case Right(tweet) => tweet })


// 1) Tweets y Retweets por dia
def numTweetsDia (ffData: List[Tuple3[Int,String,String]],isReTweet:Boolean):Int = {
  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isReTweet)
    (countersList.flatMap(t2 => List(t2._1)).filterNot(x=>x.startsWith("RT")).length)
  else
    (countersList.flatMap(t2 => List(t2._2)).filter(x=>x.startsWith("RT")).length)
}
val tweetsyRetweetsDia = values.map(tweet => (tweet.time.getDayOfMonth,tweet.text,tweet.text)).
  groupBy(_._1).map(kv=>(kv._1,
  numTweetsDia(kv._2,true),
  numTweetsDia(kv._2, false)
))
//Crear csv
val out = java.io.File.createTempFile("1TweetsyRetweetsDia",".csv")
val writer = out.asCsvWriter[(Int,Int,Int)](rfc.withHeader("days","tweets","retweets"))
tweetsyRetweetsDia.foreach(writer.write(_))
writer.close()


// 2) Tweets y Retweets por hora
def numTweetsHora (ffData: List[Tuple3[Int,String,String]],isReTweet:Boolean):Int = {
  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isReTweet)
    (countersList.flatMap(t2 => List(t2._1)).filterNot(x=>x.startsWith("RT")).length)
  else
    (countersList.flatMap(t2 => List(t2._2)).filter(x=>x.startsWith("RT")).length)
}
val tweetsyRetweetsHora = values.map(tweet => (tweet.time.getHour,tweet.text,tweet.text)).
  groupBy(_._1).
  map(kv=>(kv._1,
    numTweetsHora(kv._2,true),
    numTweetsHora(kv._2, false)
  ))
//Crear csv
val out = java.io.File.createTempFile("2TweetsyRetweetsHora",".csv")
val writer = out.asCsvWriter[(Int,Int,Int)](rfc.withHeader("hours","tweets","retweets"))
tweetsyRetweetsHora.foreach(writer.write(_))
writer.close()


// 3) Aplicaciones más utilizadas para publicar Tweets
val remplazar = values.map(tweet => tweet.source.replace("</a>",""))
val appsUtilizadas = ListMap(remplazar.map(x => x.split(">").last)
  .groupBy(identity).map({case(k,v) => (k, v.length)})
  .toSeq.sortWith(_._2 > _._2):_*).filter(_._2 > 10)
val out = java.io.File.createTempFile("3AppsUtilizadas",".csv")
val writer = out.asCsvWriter[(String,Int)](rfc.withHeader("apps","count"))
appsUtilizadas.foreach(writer.write(_))
writer.close()



// 4) Distribución de Hashtags
val distribucionHashtags = ListMap(values.map(tweet => ujson.read(tweet.entitiesStr).obj("hashtags").arr.length)
  .groupBy(identity).map({case(k,v) => (k,v.length)})
  .toSeq.sortWith(_._2 > _._2):_*)
//Crear csv
val out = java.io.File.createTempFile("4DistribucionDeHashtags", ".csv")
val writer = out.asCsvWriter[(Int, Int)] (rfc.withHeader("hashtags", "count"))
distribucionHashtags.foreach(writer.write(_))
writer.close()


// 5) Distribución de Menciones
val distribucionMentions = ListMap(values.map(tweet => ujson.read(tweet.entitiesStr).obj("user_mentions").arr.length).
  groupBy(identity).map({case(k,v) => (k,v.length)})
  .toSeq.sortWith(_._2 > _._2):_*)
//Crear csv
val out = java.io.File.createTempFile("5DistribucionDeMenciones", ".csv")
val writer = out.asCsvWriter[(Int, Int)] (rfc.withHeader("mentions", "count"))
distribucionMentions.foreach(writer.write(_))
writer.close()


// 6) Distribución de URLs
val distribucionURLs = ListMap(values.map(tweet => ujson.read(tweet.entitiesStr).obj("urls").arr.length).
  groupBy(identity).map({case(k,v)=> (k,v.length)})
  .toSeq.sortWith(_._2 > _._2):_*)
//Crear csv
val out = java.io.File.createTempFile("6DistribucionDeURLs", ".csv")
val writer = out.asCsvWriter[(Int, Int)] (rfc.withHeader("urls", "count"))
distribucionURLs.foreach(writer.write(_))
writer.close()


// 7) Distribución de Media
val distribucionMedia = ListMap(values.map(tweet => Try(ujson.read(tweet.entitiesStr).obj("media")) match {
  case Success(v)=>"Tweets con multimedias"
  case Failure(e)=>"Tweets sin multimedias"})
  .groupBy(identity).map({case(k,v) => (k , v.length)})
  .toSeq.sortWith(_._2 > _._2):_*)
//Crear csv
val out = java.io.File.createTempFile("7DistribucionDeMedia", ".csv")
val writer = out.asCsvWriter[(String, Int)] (rfc.withHeader("media", "count"))
distribucionMedia.foreach(writer.write(_))
writer.close()


// 8) ¿Existe una correlación entre el número de amigos y la cantidad de seguidores?
val list= values.collect( { case tweet => (tweet.userFollowersCount,tweet.userFriendsCount) })
def Pearson (tabla: List[(Double, Double)] ): Double ={
  val mediaX= (tabla. map(a => a._1).sum.toDouble/tabla.length)
  val mediaY= (tabla. map(a => a._2).sum.toDouble/tabla.length)
  val covarianza = ((tabla. map(a => a._1 *a._2).sum )/ tabla.length) -(mediaX *mediaY)
  val desX= Math.sqrt((tabla. map(x => Math.pow(x._1,2)).sum/ tabla.length)- Math.pow(mediaX,2))
  val desY =Math.sqrt((tabla. map(y => Math.pow(y._2,2)).sum/ tabla.length)- Math.pow(mediaY,2))
  covarianza/(desX*desY)
}
val correlacionP = Pearson(list)



/* 9) El comportamiento de los usuarios. Por cada usuario se debe presentar:
la cantidad de seguidores y de amigos, también el número de Tweets y re-tweets*/
def processFFCounters(ffData: List[Tuple3[String,Double,Double]],isFollowers:Boolean):Int = {
  val avg = (nums:List[Double])=> nums.sum / nums.length
  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isFollowers)
    avg(countersList.flatMap(t2 => List(t2._1))).toInt
  else
    avg(countersList.flatMap(t2 => List(t2._2))).toInt
}
def processFFCounters2(ffData: List[Tuple3[String,String,String]],isReTweet:Boolean):Int = {
  val countersList = ffData.map(t3 => (t3._2,t3._3))
  if(isReTweet)
    (countersList.flatMap(t2 => List(t2._1)).filter(x=>x.startsWith("RT"))).length
  else
    (countersList.flatMap(t2 => List(t2._2)).filterNot(x=>x.startsWith("RT"))).length
}
def completo(ffData: List[Tuple5[String, Double, Double, String, String]],isTrue:Boolean,tweetOrFriends:Boolean):Int = {
  val datos1 = ffData.map(x=>(x._1,x._2,x._3))
  val datos2 = ffData.map(x=>(x._1,x._4,x._5))
  if(tweetOrFriends:Boolean)
    processFFCounters(datos1,isTrue)
  else
    processFFCounters2(datos2,isTrue)
}
val consulta =  values.map(tweet => (tweet.fromUser,tweet.userFollowersCount, tweet.userFriendsCount,tweet.text,tweet.text)).
  groupBy(_._1).map(kv=>(kv._1,
  completo(kv._2,true,true),
  completo(kv._2, false,true),
  completo(kv._2,true,false),
  completo(kv._2, false,false)))
//Crear csv
val out = java.io.File.createTempFile("9Comportamiento",".csv")
val writer = out.asCsvWriter[(String,Int,Int,Int,Int)](rfc.withHeader("usuario","followers","friends","tweet","retweets"))
consulta.foreach(writer.write(_))
writer.close()


// 10) Cuántas veces se ha mencionado a un usuario.
val mentions = values.flatMap(tweet => ujson.read(tweet.entitiesStr).obj("user_mentions").arr)
  .map(ht => ht.obj("screen_name").str).groupBy(identity).map({case(k,v) => (k,v.length)})
//Crear csv
val out = java.io.File.createTempFile("10UsuariosMencionados",".csv")
val writer = out.asCsvWriter[(String,Int)](rfc.withHeader("user","mentions"))
mentions.foreach(writer.write(_))
writer.close()