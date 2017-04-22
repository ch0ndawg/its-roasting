import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner
import com.datastax.spark.connector._
import java.util.Random

import scala.annotation.tailrec

object ItsRoastingApp  {
  // ALERT: This should not be hard-coded.
  val conductivity = 1.0 // global constant
  val rng = new Random

  def simulation(sc: SparkContext, ncells : Int, nsteps : Int, nprocs: Int, rateParam: Double = 1.0, probParam: Double = 0.1, leftX: Double = -10.0, rightX: Double = 10.0,
               bottomY: Double = -10.0, topY :Double = 10.0, sigma: Double = 3.0, ao: Double = 1.0, coeff: Double = 0.1875) : Unit = {
    val dx = (rightX-leftX)/(ncells-1) // determine spatial step size
    val dy = (topY - bottomY)/(ncells-1)
    
    // REPLACE this with vector of values from the previous timestep
    def tempFromIdx(i: Int,j:Int) : ((Int,Int), Double)= {
      val x = leftX + dx*i +dx/2.0
      val y = bottomY + dy*j+ dy/2.0
      ((i,j),0.0) //ao*math.exp(-(x*x+y*y)/(2.0*sigma*sigma)))
    }
    // possibly replace this with ghost cells
    def interior(idx : ((Int,Int),Double) ): Boolean =
    { idx._1._1 >0 && idx._1._1 < ncells-1 && idx._1._2>0 && idx._1._2<ncells-1 }
    
    // stencil takes the place of sparse matrix arithmetic
    def stencil(currentVal: ((Int,Int),Double), f : Vector[Double] ) = {
      val k = conductivity
      val ((i,j),u) = currentVal // REPLACE with ((i,j),item) for production, or even ((i,j,k), item) if time permits
      
      // normalized inhomogeneous term
      val dtf = if (rng.nextDouble < probParam) coeff*dx*dx/k *rateParam
      	      	else 0.0 // streaming data at timestep; coeff is k dt/dx^2
      	      	
     // produce the varicous increments using the stencil (this is the "matrix multiplication")
      val incrementVals = Vector(((i,j), -4*coeff*u), ((i-1,j), coeff*u), ((i+1,j), coeff*u),
                                 ((i,j-1), coeff*u), ((i,j+1),coeff*u), ((i,j), dtf)) // also add in inhomogeneous term
      
      
      // get rid of the boundary points, and tack on the original item
      // this is perfectly set up for a reduceByKey
      (incrementVals filter interior) :+ currentVal
    }
    
    val grid = for { i <- 0 until ncells
                     j <- 0 until ncells 
                    } yield (i,j)
    
    val temp = grid map (idx => tempFromIdx(idx._1,idx._2)) // REPLACE with: initial condition
    // REPLACE : Read up on Range Partitioning, and curse them for making the Python so different from Scala
    val tempParallel = sc.parallelize(temp)
    val rangePartitioner = new RangePartitioner(nprocs, tempParallel)
    val repartitionedTemp = tempParallel.partitionBy(rangePartitioner).persist()
    
    def timeStep(u: org.apache.spark.rdd.RDD[((Int,Int),Double)], step: Int) = {
    	// WRITE u to database
      // format correctly
    	val dbu = u map { case ((i,j),t) => (step, leftX + dx*i +dx/2.0, bottomY + dy*j + dy/2.0, t) }
    	dbu.saveToCassandra("heatgen", "temps", SomeColumns("time", "x_coord", "y_coord","temp"))
    
    	// COMPUTE next timestep
      val newStreamingData = Vector[Double]() // REPLACE WITH Kafka stream
      val stencilParts = u flatMap (stencil(_,newStreamingData))
    	 // maintain the range partitioner
      stencilParts.reduceByKey(rangePartitioner, _+_).persist()
    }
                    
    // executes all timesteps in a fold operation. Because our folding function has the side effect
    // of writing to the database, all the intermediate values are actually saved
    //  we could also do a scanLeft on a stream. That would be more hipster!
    val finalU = (0 until nsteps).foldLeft(repartitionedTemp)(timeStep)
    finalU.collect().foreach(println)
  }
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    simulation(sc,
               args(0).toInt /* resolution: number of cells */,
               args(1).toInt /* time in deciframes (1/600) */,
               args(2).toInt /* number of processes */,
	       args(3).toDouble,
	       args(4).toDouble)
  }
}