import slick.jdbc.MySQLProfile.api._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object Main extends App {

  val db = Database.forConfig("mysqlDB")
  // represents the actual table on which we will be building queries on
  val peopleTable = TableQuery[People]

  val dropPeopleCmd = DBIO.seq(peopleTable.schema.drop)
  val initPeopleCmd = DBIO.seq(peopleTable.schema.create)

  def dropDB = {
    //do a drop followed by initialisePeople
    val dropFuture = Future{
      db.run(dropPeopleCmd)
    }
    //Attempt to drop the table, Await does not block here
    Await.result(dropFuture, Duration.Inf).andThen{
      case Success(_) =>  initialisePeople
      case Failure(error) => println("Dropping the table failed due to: " + error.getMessage)
        initialisePeople
    }
  }

  def initialisePeople = {
    //initialise people
    val setupFuture =  Future {
      db.run(initPeopleCmd)
    }
    //once our DB has finished initializing we are ready to roll, Await does not block
    Await.result(setupFuture, Duration.Inf).andThen{
      case Success(_) => runQuery
      case Failure(error) => println("Initialising the table failed due to: " + error.getMessage)
    }
  }

//  def message(number : Int): Int = number match {
//    case number:Int if (number == 1) =>
//  }

  def runQuery = {
    val insertPeople = Future {
      val query = peopleTable ++= Seq(
        (10, "Jack", "Wood", 36),
        (20, "Tim", "Brown", 24)
      )
      println(query.statements.head)
      db.run(query)
    }

    Await.result(insertPeople, Duration.Inf).andThen {
      case Success(_) => updateAgeQuery(1,"Jack","Wood",36)
      case Failure(error) => println("Welp! Something went wrong! " + error.getMessage)
    }
  }

  def createQuery(id: Int, fName: String, lName: String, age: Int) = {
    val createPeople = Future {
      val query = peopleTable ++= Seq(
        (id,fName,lName,age)
      )
      db.run(query)
    }

    Await.result(createPeople,Duration.Inf).andThen {
      case Success(_) => db.close(); println(s"Create query successful:\n ${(id/10).toInt} $fName $lName $age")
      case Failure(error) => println("Welp! Something went wrong! " + error.getMessage)
    }
  }

  def readQuery(fName: String) = {
    val queryFuture = Future {
      val query = peopleTable.filter(_.fName === fName).result
      db.run(query).map(_.foreach {
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")})
    }

    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) =>  db.close()  //cleanup DB connection
      case Failure(error) => println("Read query failed due to" + error.getMessage)
    }
  }

  def listPeople = {
    val queryFuture = Future {
      // simple query that selects everything from People and prints them out
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")})
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) =>  db.close()  //cleanup DB connection
      case Failure(error) => println("Listing people failed due to: " + error.getMessage)
    }
  }

  def updateAgeQuery(id: Int, fName: String, lName: String, age: Int) = {
    val updateQuery = Future {
      db.run(peopleTable.filter(_.id === id).delete)
      val query = peopleTable ++= Seq(
        (id*10,fName,lName,age+1)
      )
      db.run(query)
    }
    Await.result(updateQuery,Duration.Inf).andThen {
      case Success(_) => println("Update successful"); db.close()
      case Failure(error) => println("Unsuccessful update due to:" + error.getMessage)
    }

  }

  def deletePeople(idNum: Int) = {
    val queryDeleteFuture = Future {
      val action = peopleTable.filter(_.id === idNum).delete
      println(action.statements.head)
      db.run(action)
    }
    Await.result(queryDeleteFuture, Duration.Inf).andThen {
      case Success(_) => Thread.sleep(1000); listPeople; println("Deleted successful")
      case Failure(error) => println("Deleting people failed due to: " + error.getMessage)
    }
  }

  dropDB
  Thread.sleep(10000)

}
