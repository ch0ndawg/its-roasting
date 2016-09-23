import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner
import com.datastax.spark.connector._

object ItsRoastingApp  {
  val conf = new SparkConf().setMaster("local").setAppName("My App")
  val sc = new SparkContext(conf)
  val conductivity = 1.0 // global constant
  
  def simulation(/*sc*/ ncells : Int, nsteps : Int, nprocs: Int, leftX: Double = -10.0, rightX: Double = 10.0,
               sigma: Double = 3.0, ao: Double = 1.0, coeff: Double = 0.375) : Unit = {
    val dx = (rightX-leftX)/(ncells-1) // determine spatial step size
    
    // REPLACE this with vector of values from the previous timestep
    def tempFromIdx(i: Int) : (Int, Double)= {
      val x = leftX + dx*i +dx/2.0
      (i, ao*math.exp(-x*x/(2.0*sigma*sigma)))
    }
    // possibly replace this with ghost cells
    def interior(idx : (Int,Double) ) = {
    	 idx._1 > 0 && idx._1 < ncells-1
    }
    
    // stencil takes the place of sparse matrix arithmetic
    def stencil(currentVal: (Int,Double), f : Vector[Double] ) = {
      val k = conductivity
      val (i,u) = currentVal // REPLACE with ((i,j),item) for production, or even ((i,j,k), item) if time permits
      
      // normalized inhomogeneous term
      val dtf = 0.0//coeff*dx*dx/k * f(i) // streaming data at timestep; coeff is k dt/dx^2
      
      // produce the varicous increments using the stencil (this is the "matrix multiplication")
      val incrementVals = Vector((i, -2*coeff*u), (i-1,coeff*u), (i+1,coeff*u), (i, dtf)) // also add in inhomogeneous term
      
      
      // get rid of the boundary points, and tack on the original item
      // this is perfectly set up for a reduceByKey
      (incrementVals filter interior) :+ currentVal
    }
    
    val temp = (0 until ncells) map tempFromIdx // REPLACE with: initial condition
    // REPLACE : Read up on Range Partitioning, and curse them for making the Python so different from Scala
    val tempParallel = sc.parallelize(temp)//partitionBy(new RangePartitioner(nprocs))
    val rangePartitioner = new RangePartitioner(nprocs,tempParallel)
    
    // TAIL recursion method, in case Spark barks at me for using "var currentTemp ="
    
    
      // val db = sc.cassandraTable("heatgen","temps")
      //@tailrec
      def timeStep(u : org.apache.spark.rdd.RDD[(Int,Double)], niter: Int) : Unit = {
      	// WRITE u to database
        // format correctly
      	val dbu = u map {
      	  case (i,u) => Seq(("time", nsteps - niter), ("x_coord",leftX + dx*i +dx/2.0),
      	                    ("y_coord",0.0), ("temp", u))
      	}
      	dbu.saveToCassandra("heatgen", "temps", SomeColumns("time", "x_coord", "y_coord","temp"))
      
      	// compute next timestep
        val newStreamingData = Vector[Double]() // REPLACE WITH Kafka stream
        val stencilParts = u.flatMap(stencil(_,newStreamingData))
        val newU = stencilParts.reduceByKey(_+_)
        // Possibly set up a new Range Partitioner for it here
        if (niter > 0) timeStep(newU.partitionBy(rangePartitioner), niter-1)
        else newU.collect().foreach(println) //WRITE newTemp to database or just print
      }
      timeStep(tempParallel.partitionBy(rangePartitioner) ,nsteps)
  }
  def main(args: Array[String]) {
    simulation(100, 20, 4)
  }
}